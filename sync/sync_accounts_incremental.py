# sync_accounts_incremental.py
import os
import time
import requests
import psycopg2
from psycopg2.extras import Json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

ACCOUNTS_BASE = "https://accounts.zoho.com"
API_BASE = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")
SELF_ID = os.getenv("ZOHO_SELF_CLIENT_ID")
SELF_SECRET = os.getenv("ZOHO_SELF_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")

MODULE = "Accounts"
FIELDS = "id,Account_Name,Owner,Created_Time,Modified_Time"
PER_PAGE = 200

def get_access_token():
    r = requests.post(
        f"{ACCOUNTS_BASE}/oauth/v2/token",
        data={
            "grant_type": "refresh_token",
            "client_id": SELF_ID,
            "client_secret": SELF_SECRET,
            "refresh_token": REFRESH_TOKEN,
        },
        timeout=30,
    )
    r.raise_for_status()
    js = r.json()
    if "access_token" not in js:
        raise RuntimeError(f"Error al renovar token: {js}")
    return js["access_token"]

def connect_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )

def ensure_state_table(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.crm_sync_state (
          module TEXT PRIMARY KEY,
          last_modified TIMESTAMPTZ NOT NULL,
          updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)

def get_start_cursor(conn):
    """Lee cursor de crm_sync_state; si no existe, usa MAX(modified_time) local; si no hay, epoch."""
    with conn, conn.cursor() as cur:
        cur.execute("SELECT last_modified FROM public.crm_sync_state WHERE module=%s", (MODULE,))
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute("SELECT MAX(modified_time) FROM public.crm_accounts;")
        row = cur.fetchone()
        if row and row[0]:
            return row[0]
    # epoch (UTC)
    return datetime(1970, 1, 1, tzinfo=timezone.utc)

def save_cursor(conn, dt):
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO public.crm_sync_state (module, last_modified)
            VALUES (%s, %s)
            ON CONFLICT (module) DO UPDATE
            SET last_modified = GREATEST(public.crm_sync_state.last_modified, EXCLUDED.last_modified),
                updated_at = now();
        """, (MODULE, dt))

def fetch_page(token, ims_dt, page_token=None):
    params = {"per_page": PER_PAGE, "fields": FIELDS, "sort_by": "Modified_Time", "sort_order": "asc"}
    if page_token:
        params["page_token"] = page_token
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        # Zoho acepta formato ISO 8601 con zona (+00:00, -03:00, etc.)
        "If-Modified-Since": ims_dt.isoformat()
    }
    r = requests.get(f"{API_BASE}/crm/v5/Accounts", headers=headers, params=params, timeout=60)
    if r.status_code == 304:
        return [], None, False  # sin cambios
    if r.status_code == 429:
        time.sleep(2)
        return fetch_page(token, ims_dt, page_token)
    r.raise_for_status()
    js = r.json()
    data = js.get("data", []) or []
    info = js.get("info", {}) or {}
    return data, info.get("next_page_token"), bool(info.get("more_records"))

def upsert_batch(conn, rows):
    sql = """
    INSERT INTO public.crm_accounts
      (zoho_id, account_name, owner_id, owner_name, owner_email,
       created_time, modified_time, raw_json)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (zoho_id) DO UPDATE SET
       account_name = EXCLUDED.account_name,
       owner_id     = EXCLUDED.owner_id,
       owner_name   = EXCLUDED.owner_name,
       owner_email  = EXCLUDED.owner_email,
       created_time = EXCLUDED.created_time,
       modified_time= EXCLUDED.modified_time,
       raw_json     = EXCLUDED.raw_json,
       synced_at    = now();
    """
    with conn.cursor() as cur:
        for rec in rows:
            owner = rec.get("Owner") or {}
            cur.execute(sql, (
                rec.get("id"),
                rec.get("Account_Name"),
                owner.get("id"),
                owner.get("name"),
                owner.get("email"),
                rec.get("Created_Time"),
                rec.get("Modified_Time"),
                Json(rec),
            ))

def sync_incremental():
    token = get_access_token()
    conn = connect_db()
    ensure_state_table(conn)

    # Cursor inicial (le restamos 1 segundo por seguridad ante empates de milisegundos)
    start = get_start_cursor(conn) - timedelta(seconds=1)
    print(f"Iniciando incremental desde: {start.isoformat()}")

    total = 0
    page_token = None
    max_seen = start

    try:
        while True:
            rows, next_token, more = fetch_page(token, start, page_token)
            if not rows and not more and not page_token:
                print("No hay cambios nuevos.")
                break

            with conn:
                upsert_batch(conn, rows)

            # Avanzar cursor local con el mayor Modified_Time visto
            for rec in rows:
                mt = rec.get("Modified_Time")
                if mt:
                    # PostgreSQL convertirá ISO 8601 sin problema; pero para el cursor lo parseamos
                    # y nos quedamos con el máximo
                    try:
                        # fromisoformat soporta 'YYYY-MM-DDTHH:MM:SS±HH:MM'
                        dt = datetime.fromisoformat(mt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if dt > max_seen:
                            max_seen = dt
                    except Exception:
                        pass

            total += len(rows)
            print(f"Lote: {len(rows)} registros (acumulado: {total})")

            if not more:
                break
            page_token = next_token
            time.sleep(0.2)

        # Guardamos cursor solo si avanzamos
        if max_seen > start:
            save_cursor(conn, max_seen)
            print(f"Cursor actualizado a: {max_seen.isoformat()}")

    finally:
        conn.close()

    print(f"Incremental terminado. Registros procesados: {total}")

if __name__ == "__main__":
    sync_incremental()
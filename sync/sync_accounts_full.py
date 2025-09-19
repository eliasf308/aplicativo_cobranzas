# sync_accounts_full.py
import os
import time
import requests
import psycopg2
from psycopg2.extras import Json
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

def fetch_page(token, page_token=None):
    params = {"per_page": PER_PAGE, "fields": FIELDS}
    if page_token:
        params["page_token"] = page_token
    r = requests.get(
        f"{API_BASE}/crm/v5/Accounts",
        headers={"Authorization": f"Zoho-oauthtoken {token}"},
        params=params,
        timeout=60,
    )
    if r.status_code == 429:
        # Límite momentáneo: esperar y reintentar
        time.sleep(2)
        return fetch_page(token, page_token)
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
            cur.execute(
                sql,
                (
                    rec.get("id"),
                    rec.get("Account_Name"),
                    owner.get("id"),
                    owner.get("name"),
                    owner.get("email"),
                    rec.get("Created_Time"),
                    rec.get("Modified_Time"),
                    Json(rec),
                ),
            )

def full_sync():
    token = get_access_token()
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    total = 0
    page_token = None
    page_num = 0
    try:
        while True:
            page_num += 1
            rows, next_token, more = fetch_page(token, page_token)
            if not rows:
                break
            with conn:
                upsert_batch(conn, rows)
            total += len(rows)
            print(f"Página {page_num}: {len(rows)} registros (acumulado: {total})")
            if not more:
                break
            page_token = next_token
            # Pequeña pausa defensiva
            time.sleep(0.2)
    finally:
        conn.close()
    print(f"Sync completo. Registros procesados: {total}")

if __name__ == "__main__":
    full_sync()

# -*- coding: utf-8 -*-
"""
Incremental sync de Zoho CRM Deals -> Postgres (public.crm_deals)

Mejoras:
- Guarda raw_json (si existe la columna).
- Booleanos omitidos por Zoho se graban como False (evita NULLs).
- ON CONFLICT actualiza también synced_at=now() si la columna existe.
- Mantiene detección automática de PK/UNIQUE y fetch por IDs sin 'fields' (evita 400).

Requiere .env:
  ZOHO_API_DOMAIN, ZOHO_SELF_CLIENT_ID, ZOHO_SELF_CLIENT_SECRET, ZOHO_REFRESH_TOKEN
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

import json
import pathlib
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras as pgx
import requests
from dotenv import dotenv_values

# ------------------ Config ------------------

TABLE_SCHEMA = "public"
TABLE_BASENAME = "crm_deals"
TABLE_NAME = f"{TABLE_SCHEMA}.{TABLE_BASENAME}"

STATE_DIR = pathlib.Path('sync') / '_state'
STATE_DIR.mkdir(parents=True, exist_ok=True)
CURSOR_FILE = STATE_DIR / 'deals_cursor.txt'

PER_PAGE = 200
BULK_IDS_CHUNK = 100
UPSERT_CHUNK = 200
SAFE_LIST_FIELDS = ["id", "Modified_Time"]
SESSION_TIMEOUT = 90

# ------------------ Utilitarios ------------------

def load_env():
    env = dotenv_values('.env')
    required = [
        'ZOHO_API_DOMAIN', 'ZOHO_SELF_CLIENT_ID', 'ZOHO_SELF_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN',
        'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'
    ]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")
    return env

def get_access_token(env):
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": env["ZOHO_SELF_CLIENT_ID"],
        "client_secret": env["ZOHO_SELF_CLIENT_SECRET"],
        "refresh_token": env["ZOHO_REFRESH_TOKEN"],
    }
    r = requests.post(url, data=data, timeout=SESSION_TIMEOUT)
    r.raise_for_status()
    return r.json()["access_token"]

def to_rfc1123(iso_str: str) -> str:
    if not iso_str:
        raise ValueError("iso_str vacío")
    s = iso_str
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s).astimezone(timezone.utc)
    return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')

def read_cursor_default_epoch() -> str:
    if CURSOR_FILE.exists():
        val = CURSOR_FILE.read_text(encoding='utf-8').strip()
        if val:
            return val
    return "1970-01-01T00:00:00+00:00"

def save_cursor(iso_utc: str):
    CURSOR_FILE.write_text(iso_utc, encoding='utf-8')

def pg_connect(env):
    conn = psycopg2.connect(
        host=env["DB_HOST"],
        port=int(env["DB_PORT"]),
        dbname=env["DB_NAME"],
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
    )
    conn.autocommit = False
    return conn

def get_existing_columns(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, COALESCE(udt_name, data_type) AS t
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (TABLE_SCHEMA, TABLE_BASENAME))
        return {name: (t or "").lower() for name, t in cur.fetchall()}

def detect_conflict_constraint(conn):
    sql = """
    WITH cons AS (
      SELECT c.conname, c.contype, a.attname, cols.ord
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
      WHERE n.nspname = %s AND t.relname = %s AND c.contype IN ('p','u')
    )
    SELECT conname, MIN(contype) AS contype, ARRAY_AGG(attname ORDER BY ord) AS cols
    FROM cons
    GROUP BY conname
    ORDER BY contype ASC, conname ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (TABLE_SCHEMA, TABLE_BASENAME))
        rows = cur.fetchall()
    if not rows:
        return (None, [])
    conname, contype, cols = rows[0]
    return (conname, cols)

# ------------------ Zoho helpers ------------------

def fetch_page_deals(env, token, since_iso, page_token):
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals'
    headers = {"Authorization": f"Zoho-oauthtoken {token}", "If-Modified-Since": to_rfc1123(since_iso)}
    params = {"per_page": PER_PAGE, "sort_by": "Modified_Time", "sort_order": "asc", "fields": ",".join(SAFE_LIST_FIELDS)}
    if page_token:
        params["page_token"] = page_token
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code == 204:
        return [], {"more_records": False}
    if r.status_code == 400:
        print(f"[WARN] /Deals 400 (listado). Body: {r.text[:800]}")
        raise requests.HTTPError("400 on /Deals list", response=r)
    r.raise_for_status()
    j = r.json()
    return j.get("data", []) or [], j.get("info", {}) or {}

def fetch_page_search(env, token, since_iso, page_token):
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals/search'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    page = 1
    if page_token and str(page_token).startswith("search:"):
        try:
            page = int(str(page_token).split(":", 1)[1])
        except:
            page = 1
    params = {"per_page": PER_PAGE, "page": page, "criteria": f"(Modified_Time:after:{since_iso})", "fields": ",".join(SAFE_LIST_FIELDS)}
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code == 204:
        return [], {"more_records": False}
    if r.status_code == 400:
        params.pop("sort_by", None); params.pop("sort_order", None)
        r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    r.raise_for_status()
    j = r.json()
    data = j.get("data", []) or []
    info = j.get("info", {}) or {}
    more = bool(info.get("more_records"))
    next_token = f"search:{page+1}" if more else None
    return data, {"more_records": more, "next_page_token": next_token}

def fetch_page(env, token, since_iso, page_token):
    if page_token and str(page_token).startswith("search:"):
        return fetch_page_search(env, token, since_iso, page_token)
    try:
        return fetch_page_deals(env, token, since_iso, page_token)
    except requests.HTTPError:
        return fetch_page_search(env, token, since_iso, None)

def fetch_deals_by_ids(env, token, ids):
    if not ids:
        return []
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"ids": ",".join(ids)}  # sin 'fields' => evita límite y trae todo lo disponible
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code >= 400:
        print(f"[ERROR] fetch_deals_by_ids status={r.status_code} ids={len(ids)}")
        print(f"[ERROR] url={r.url[:1000]}")
        print(f"[ERROR] body={(getattr(r,'text','') or '')[:800]}")
        r.raise_for_status()
    return (r.json().get("data") or [])

# ------------------ Mapeo a columnas ------------------

def norm_col(name: str) -> str:
    s = name.replace(" ", "_").replace("/", "_").replace("-", "_")
    s = s.replace("(", "").replace(")", "").replace(".", "").replace("%", "")
    while "__" in s:
        s = s.replace("__", "_")
    return s.lower()

def _is_json_type(t: str) -> bool:
    return t in ("json", "jsonb")

def _ensure_json_text(val):
    if val is None:
        return "null"
    if isinstance(val, (dict, list, bool, int, float)):
        return json.dumps(val, ensure_ascii=False)
    if isinstance(val, str):
        s = val.strip()
        if s.startswith("{") or s.startswith("["):
            return s
        return json.dumps(val, ensure_ascii=False)
    return json.dumps(str(val), ensure_ascii=False)

def flatten_deal(record: dict, existing_types: dict) -> dict:
    row = {}
    existing_cols = set(existing_types.keys())

    def _set(col: str, val):
        if col not in existing_cols:
            return
        t = existing_types.get(col, "")
        if _is_json_type(t):
            row[col] = _ensure_json_text(val)
        else:
            if isinstance(val, (dict, list)):
                row[col] = json.dumps(val, ensure_ascii=False)
            else:
                row[col] = val

    # ids
    if "id" in record:
        _set("id", str(record["id"]))
    if "zoho_id" in existing_cols and "id" in record:
        _set("zoho_id", str(record["id"]))

    # timestamps base
    for api_k, col_k in [("Created_Time", "created_time"), ("Modified_Time", "modified_time")]:
        if api_k in record and record[api_k]:
            _set(col_k, record[api_k])

    # Owner
    owner = record.get("Owner")
    if isinstance(owner, dict):
        _set("owner_id", owner.get("id"))
        _set("owner_name", owner.get("name"))
        _set("owner_email", owner.get("email"))

    # resto de fields genérico
    for k, v in record.items():
        if k in ("id", "Owner", "Created_Time", "Modified_Time"):
            continue
        base_col = norm_col(k)

        # lookups
        if isinstance(v, dict) and ("id" in v or "name" in v or "email" in v):
            _set(f"{base_col}_id", v.get("id"))
            _set(f"{base_col}_name", v.get("name"))
            _set(f"{base_col}_email", v.get("email"))
            if base_col in existing_cols and base_col not in row:
                _set(base_col, v.get("name") or v.get("id"))
            continue

        # listas
        if isinstance(v, list):
            if base_col in existing_cols:
                _set(base_col, v)
            if v and isinstance(v[0], dict):
                _set(f"{base_col}_ids", "|".join([str(x.get("id") or "") for x in v]))
                _set(f"{base_col}_names", "|".join([str(x.get("name") or "") for x in v]))
            continue

        # dict genérico
        if isinstance(v, dict):
            if base_col in existing_cols:
                _set(base_col, v)
            continue

        # primitivos
        _set(base_col, v)

    # raw_json completo si existe la columna
    if "raw_json" in existing_cols:
        _set("raw_json", record)

    # default FALSE para booleanos omitidos
    for col, t in existing_types.items():
        if col not in row and t in ("bool", "boolean"):
            row[col] = False

    return row

# ------------------ Upsert ------------------

def chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_rows(conn, rows: list, existing_cols: set, conflict_conname: str | None):
    if not rows:
        return
    if not conflict_conname:
        raise RuntimeError(
            "No se encontró PK/UNIQUE en la tabla public.crm_deals. "
            "Definí una clave única (ej. PRIMARY KEY en 'id' o 'zoho_id') y volvé a ejecutar."
        )

    # columnas presentes (intersección con la tabla)
    all_keys = set()
    for r in rows:
        all_keys |= set(r.keys())
    cols = [c for c in sorted(all_keys) if c in existing_cols]
    if not cols:
        return

    cols_sql = ",".join(f'"{c}"' for c in cols)
    update_assign = ",".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c != "id")
    if "synced_at" in existing_cols:
        update_assign += ', "synced_at"=now()'

    with conn.cursor() as cur:
        for batch in chunked(rows, UPSERT_CHUNK):
            values = [[r.get(c) for c in cols] for r in batch]
            pgx.execute_values(
                cur,
                f'INSERT INTO {TABLE_NAME} ({cols_sql}) VALUES %s '
                f'ON CONFLICT ON CONSTRAINT "{conflict_conname}" DO UPDATE SET {update_assign}',
                values
            )
    conn.commit()

# ------------------ Main ------------------

def main():
    env = load_env()
    token = get_access_token(env)

    since_iso = read_cursor_default_epoch()
    print(f"Iniciando incremental Deals desde: {since_iso}")

    conn = pg_connect(env)
    try:
        existing_types = get_existing_columns(conn)
        existing_cols = set(existing_types.keys())

        conname, concols = detect_conflict_constraint(conn)
        if conname:
            print(f"Constraint de conflicto detectado: {conname} sobre columnas {concols}")
        else:
            print("No hay PK/UNIQUE en public.crm_deals — se abortará antes del upsert.")

        page_token = None
        total_ids = 0
        ids_buffer = []
        max_modified_seen = since_iso

        while True:
            data, info = fetch_page(env, token, since_iso, page_token)
            if not data:
                break

            for rec in data:
                rid = str(rec.get("id"))
                if rid:
                    ids_buffer.append(rid); total_ids += 1
                mtime = rec.get("Modified_Time")
                if mtime and mtime > max_modified_seen:
                    max_modified_seen = mtime

            if len(ids_buffer) >= BULK_IDS_CHUNK or not info.get("more_records"):
                for group in chunked(ids_buffer, BULK_IDS_CHUNK):
                    expanded = fetch_deals_by_ids(env, token, group)
                    rows = [flatten_deal(rec, existing_types) for rec in expanded]
                    upsert_rows(conn, rows, existing_cols, conname)
                ids_buffer = []

            page_token = info.get("next_page_token")
            if not page_token and info.get("more_records"):
                page_token = info.get("next_page_token")
            if not page_token and not info.get("more_records"):
                break

        if max_modified_seen and max_modified_seen > since_iso:
            save_cursor(max_modified_seen)
            print(f"Cursor actualizado a: {max_modified_seen}")

        print(f"Incremental Deals terminado. IDs procesados: {total_ids}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()

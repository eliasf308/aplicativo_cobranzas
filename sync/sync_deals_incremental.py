# -*- coding: utf-8 -*-
"""
Incremental sync de Zoho CRM Deals -> Postgres (public.crm_deals)

Cambios clave:
- Mapeo 'zoho_id' <- 'id' (si existe la columna).
- fetch_deals_by_ids SIN 'fields' (evita 400 por URLs largas).
- Columnas JSON/JSONB se serializan a JSON válido antes del upsert.
- Detección automática del constraint para ON CONFLICT (PK/UNIQUE). Si no existe, aborta con mensaje.

Requisitos .env:
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

PER_PAGE = 200                # Tamaño de página Zoho
BULK_IDS_CHUNK = 100          # Zoho admite hasta 100 ids
UPSERT_CHUNK = 200            # Lote de insert/update
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
    """
    Devuelve dict nombre_columna -> tipo_base (json/jsonb/text/etc.)
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (TABLE_SCHEMA, TABLE_BASENAME))
        types = {}
        for name, data_type, udt in cur.fetchall():
            t = (udt or data_type or "").lower()
            types[name] = t
        return types

def detect_conflict_constraint(conn):
    """
    Devuelve (constraint_name, columns[]) de PK/UNIQUE para la tabla.
    Prioriza PK; si no hay, usa UNIQUE. Si no existe nada, devuelve (None, []).
    """
    sql = """
    WITH cons AS (
      SELECT
        c.conname,
        c.contype,
        a.attname,
        cols.ord
      FROM pg_constraint c
      JOIN pg_class t ON t.oid = c.conrelid
      JOIN pg_namespace n ON n.oid = t.relnamespace
      JOIN unnest(c.conkey) WITH ORDINALITY AS cols(attnum, ord) ON true
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = cols.attnum
      WHERE n.nspname = %s
        AND t.relname = %s
        AND c.contype IN ('p','u')
    )
    SELECT conname,
           MIN(contype) AS contype,  -- 'p' < 'u' en orden ASCII, así priorizamos PK
           ARRAY_AGG(attname ORDER BY ord) AS cols
    FROM cons
    GROUP BY conname
    ORDER BY contype ASC, conname ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (TABLE_SCHEMA, TABLE_BASENAME))
        rows = cur.fetchall()
    if not rows:
        return (None, [])
    # Elegimos la primera: PK si existe, luego UNIQUE
    conname, contype, cols = rows[0]
    return (conname, cols)

# ------------------ Zoho helpers ------------------

def fetch_module_fields(env, token) -> list:
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/settings/fields'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"module": "Deals", "per_page": 200}
    fields, page = [], 1
    while True:
        params["page"] = page
        r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
        r.raise_for_status()
        j = r.json()
        fields.extend(j.get("fields", []) or [])
        info = j.get("info", {}) or {}
        if not info.get("more_records"):
            break
        page += 1
    return fields

def full_fields_list(fields_def) -> list:
    base = set(["id", "Created_Time", "Modified_Time", "Owner"])
    for f in fields_def:
        api = f.get("api_name")
        if api:
            base.add(api)
    return sorted(base)

def fetch_page_deals(env, token, since_iso, page_token):
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals'
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "If-Modified-Since": to_rfc1123(since_iso),
    }
    params = {
        "per_page": PER_PAGE,
        "sort_by": "Modified_Time",
        "sort_order": "asc",
        "fields": ",".join(SAFE_LIST_FIELDS),
    }
    if page_token:
        params["page_token"] = page_token
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code == 204:
        return [], {"more_records": False}
    if r.status_code == 400:
        print(f"[WARN] /Deals 400 (listado). Body: {r.text[:4000]}")
        raise requests.HTTPError("400 on /Deals list", response=r)
    r.raise_for_status()
    j = r.json()
    return j.get("data", []) or [], j.get("info", {}) or {}

def fetch_page_search(env, token, since_iso, page_token):
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals/search'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    criteria = f"(Modified_Time:after:{since_iso})"
    page = 1
    if page_token and str(page_token).startswith("search:"):
        try:
            page = int(str(page_token).split(":", 1)[1])
        except:
            page = 1

    params = {
        "per_page": PER_PAGE,
        "page": page,
        "criteria": criteria,
        "sort_by": "Modified_Time",
        "sort_order": "asc",
        "fields": ",".join(SAFE_LIST_FIELDS),
    }
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code == 204:
        return [], {"more_records": False}
    if r.status_code == 400:
        params.pop("sort_by", None)
        params.pop("sort_order", None)
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

def fetch_deals_by_ids(env, token, ids, fields):
    if not ids:
        return []
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {
        "ids": ",".join(ids),
        # "fields": ",".join(fields),  # evitamos 400 por URLs largas
    }
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code >= 400:
        body = r.text if hasattr(r, "text") else "<sin body>"
        print(f"[ERROR] fetch_deals_by_ids status={r.status_code} ids={len(ids)}")
        print(f"[ERROR] url={r.url[:1000]}")
        print(f"[ERROR] body={body[:4000]}")
        r.raise_for_status()
    r.raise_for_status()
    return (r.json().get("data") or [])

# ------------------ Mapeo a columnas ------------------

def norm_col(name: str) -> str:
    s = name.replace(" ", "_").replace("/", "_").replace("-", "_")
    s = s.replace("(", "").replace(")", "").replace(".", "").replace("%", "")
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
        if s.startswith("{") or s.startswith("[") or (s.startswith('"') and s.endswith('"')):
            return s
        return json.dumps(val, ensure_ascii=False)
    return json.dumps(str(val), ensure_ascii=False)

def flatten_deal(record: dict, existing_types: dict) -> dict:
    """
    Aplana un Deal respetando tipos de columnas:
      - json/jsonb -> JSON válido
      - otras (text/date/numeric) -> valor directo (dict/list como JSON string)
    Solo keys que EXISTEN en la tabla.
    """
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

    # base
    if "id" in record:
        _set("id", str(record["id"]))

    # poblar zoho_id si existe
    if "zoho_id" in existing_cols and "id" in record:
        _set("zoho_id", str(record["id"]))

    for base_k, col_k in [("Created_Time", "created_time"), ("Modified_Time", "modified_time")]:
        if base_k in record and record[base_k]:
            _set(col_k, record[base_k])

    # Owner
    owner = record.get("Owner")
    if isinstance(owner, dict):
        _set("owner_id", owner.get("id"))
        _set("owner_name", owner.get("name"))
        _set("owner_email", owner.get("email"))

    # otros campos
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

    print("Pidiendo definición de campos (Deals)...")
    fields_def = fetch_module_fields(env, token)
    fields_full = full_fields_list(fields_def)  # referencia, no lo usamos en ids

    since_iso = read_cursor_default_epoch()
    print(f"\nIniciando incremental Deals desde: {since_iso}")

    conn = pg_connect(env)
    try:
        existing_types = get_existing_columns(conn)
        existing_cols = set(existing_types.keys())

        # Detectar constraint para ON CONFLICT
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

            # acumulo ids y max Modified_Time
            for rec in data:
                rid = str(rec.get("id"))
                if rid:
                    ids_buffer.append(rid)
                    total_ids += 1
                mtime = rec.get("Modified_Time")
                if mtime and mtime > max_modified_seen:
                    max_modified_seen = mtime

            # ampliar por ids y upsert por lotes
            if len(ids_buffer) >= BULK_IDS_CHUNK or not info.get("more_records"):
                for group in chunked(ids_buffer, BULK_IDS_CHUNK):
                    expanded = fetch_deals_by_ids(env, token, group, fields_full)
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

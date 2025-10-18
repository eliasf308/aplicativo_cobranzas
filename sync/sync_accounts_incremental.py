# -*- coding: utf-8 -*-
"""
Incremental sync de Zoho CRM **Accounts** -> Postgres (public.crm_accounts)

- Lista registros modificados desde el último cursor (If-Modified-Since) y acumula **IDs**.
- Expande por **IDs** pidiendo **todos los fields** del módulo (obtiene el catálogo desde
  `/crm/v5/settings/fields`) respetando el **límite de 50 fields** por request:
  divide en *chunks* y **fusiona** por `id`.
- Aplana dinámicamente al esquema existente y, si la tabla tiene `raw_json`, guarda el JSON completo.
- Para columnas **boolean** que Zoho omite cuando son FALSE, graba `False` (evita `NULLs` artificiales).
- Upsert `ON CONFLICT ("zoho_id") DO UPDATE` y marca `synced_at=now()` cuando la columna existe.
- Cursor incremental se persiste en `public.crm_sync_state` (fila `module='Accounts'`).

Requiere en `.env`: ZOHO_API_DOMAIN, ZOHO_SELF_CLIENT_ID, ZOHO_SELF_CLIENT_SECRET, ZOHO_REFRESH_TOKEN,
                    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
"""

from __future__ import annotations
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

import requests
import psycopg2
import psycopg2.extras as pgx
from dotenv import dotenv_values

# ---------------- Config ----------------
TABLE_SCHEMA = "public"
TABLE_BASENAME = "crm_accounts"
TABLE_NAME = f"{TABLE_SCHEMA}.{TABLE_BASENAME}"
STATE_TABLE = "public.crm_sync_state"

PER_PAGE = 200                # tamaño de página Zoho (listado)
BULK_IDS_CHUNK = 100          # /Accounts admite hasta 100 ids por request
UPSERT_CHUNK = 200            # batch de upsert a DB
SESSION_TIMEOUT = 90
SAFE_LIST_FIELDS = ["id", "Modified_Time", "Account_Name", "Owner"]

# ---------------- Utils ----------------

def load_env() -> Dict[str, str]:
    env = dotenv_values('.env')
    required = [
        'ZOHO_API_DOMAIN', 'ZOHO_SELF_CLIENT_ID', 'ZOHO_SELF_CLIENT_SECRET', 'ZOHO_REFRESH_TOKEN',
        'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD'
    ]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")
    return env

def get_access_token(env: Dict[str, str]) -> str:
    url = "https://accounts.zoho.com/oauth/v2/token"  # ajustar si tu DC difiere
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
    s = iso_str
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s).astimezone(timezone.utc)
    return dt.strftime('%a, %d %b %Y %H:%M:%S GMT')

# ---------------- DB helpers ----------------

def pg_connect(env):
    return psycopg2.connect(
        host=env["DB_HOST"], port=int(env["DB_PORT"]), dbname=env["DB_NAME"],
        user=env["DB_USER"], password=env["DB_PASSWORD"], connect_timeout=10,
    )

def ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
              module TEXT PRIMARY KEY,
              last_modified TIMESTAMPTZ NOT NULL,
              updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
    conn.commit()

def read_cursor_default_epoch(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(f"SELECT last_modified FROM {STATE_TABLE} WHERE module=%s", ("Accounts",))
        row = cur.fetchone()
        if row and row[0]:
            return row[0].astimezone(timezone.utc).isoformat()
        # fallback: mayor Modified_Time de la tabla si existe
        cur.execute(f"SELECT MAX(modified_time) FROM {TABLE_NAME}")
        row = cur.fetchone()
        if row and row[0]:
            return row[0].astimezone(timezone.utc).isoformat()
    return "1970-01-01T00:00:00+00:00"

def save_cursor(conn, iso_utc: str):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {STATE_TABLE} (module, last_modified)
            VALUES (%s, %s)
            ON CONFLICT (module) DO UPDATE
            SET last_modified = GREATEST({STATE_TABLE}.last_modified, EXCLUDED.last_modified),
                updated_at = now()
            """,
            ("Accounts", iso_utc)
        )
    conn.commit()

# ---------------- Tabla destino: metadata ----------------

def get_existing_columns(conn) -> Dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, COALESCE(udt_name, data_type) AS t
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            """,
            (TABLE_SCHEMA, TABLE_BASENAME)
        )
        return {name: t.lower() for name, t in cur.fetchall()}

# ---------------- Zoho helpers ----------------

_FIELDS_CACHE_ACCOUNTS: Optional[List[str]] = None

def get_accounts_api_fields(env, token) -> List[str]:
    global _FIELDS_CACHE_ACCOUNTS
    if _FIELDS_CACHE_ACCOUNTS is not None:
        return _FIELDS_CACHE_ACCOUNTS
    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/settings/fields"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"module": "Accounts"}
    r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    r.raise_for_status()
    j = r.json() or {}
    _FIELDS_CACHE_ACCOUNTS = [f.get("api_name") for f in j.get("fields", []) if f.get("api_name")]
    return _FIELDS_CACHE_ACCOUNTS

# ---------------- Listado & Search ----------------

def fetch_page_accounts(env, token, since_iso: str, page_token: Optional[str]):
    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/Accounts"
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
    r.raise_for_status()
    j = r.json() or {}
    return j.get("data") or [], j.get("info") or {}

def fetch_page_search(env, token, since_iso: str, page_token: Optional[str]):
    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/Accounts/search"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    criteria = f"(Modified_Time:after:{since_iso})"
    page = 1
    if page_token and str(page_token).startswith("search:"):
        try:
            page = int(str(page_token).split(":", 1)[1])
        except Exception:
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
    if r.status_code == 400:
        # algunos data centers no permiten sort en /search
        params.pop("sort_by", None)
        params.pop("sort_order", None)
        r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
    if r.status_code == 204:
        return [], {"more_records": False}
    r.raise_for_status()
    j = r.json() or {}
    data = j.get("data") or []
    info = j.get("info") or {}
    more = bool(info.get("more_records"))
    next_token = f"search:{page+1}" if more else None
    return data, {"more_records": more, "next_page_token": next_token}

def fetch_page(env, token, since_iso, page_token):
    try:
        return fetch_page_accounts(env, token, since_iso, page_token)
    except requests.HTTPError:
        return fetch_page_search(env, token, since_iso, None)

# ---------------- Expandir por IDs (chunks ≤ 50 fields) ----------------

def fetch_accounts_by_ids_all_fields(env, token, ids: List[str]) -> List[Dict[str, Any]]:
    if not ids:
        return []
    fields_all = get_accounts_api_fields(env, token) or []
    base_extra = ["Owner", "Account_Name", "Created_Time", "Modified_Time"]

    def dedup(seq):
        seen, out = set(), []
        for x in seq:
            if x and x not in seen:
                seen.add(x); out.append(x)
        return out

    fields_no_base = [f for f in fields_all if f not in ("id",) + tuple(base_extra)]
    chunks: List[List[str]] = []
    first_room = 50 - 1 - len(base_extra)
    first_chunk_rest = fields_no_base[:max(0, first_room)]
    chunks.append(dedup(["id"] + base_extra + first_chunk_rest))
    idx = len(first_chunk_rest)
    while idx < len(fields_no_base):
        chunk_fields = ["id"] + fields_no_base[idx: idx + 49]
        chunks.append(dedup(chunk_fields))
        idx += 49

    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/Accounts"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    merged: Dict[str, Dict[str, Any]] = {}
    for fields in chunks:
        params = {"ids": ",".join(ids), "fields": ",".join(fields)}
        r = requests.get(url, headers=headers, params=params, timeout=SESSION_TIMEOUT)
        if r.status_code >= 400:
            print(f"[ERROR] fetch_accounts_by_ids_all_fields {r.status_code}: {r.text[:1000]}")
            r.raise_for_status()
        for rec in (r.json().get("data") or []):
            rid = str(rec.get("id")) if rec else None
            if not rid:
                continue
            if rid not in merged:
                merged[rid] = rec
            else:
                for k, v in rec.items():
                    if v is not None and (k not in merged[rid] or merged[rid][k] in (None, "", [])):
                        merged[rid][k] = v
    return list(merged.values())

# ---------------- Flatten ----------------

def norm_col(name: str) -> str:
    s = name.replace(" ", "_").replace("/", "_").replace("-", "_")
    s = s.replace("(", "").replace(")", "").replace(".", "").replace("%", "")
    while "__" in s:
        s = s.replace("__", "_")
    return s.lower()

_DEF_JSON_TYPES = ("json", "jsonb")

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

def flatten_account(record: Dict[str, Any], existing_types: Dict[str, str]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    existing_cols = set(existing_types.keys())

    def _set(col: str, val):
        if col not in existing_cols:
            return
        t = existing_types.get(col, "")
        if t in _DEF_JSON_TYPES:
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

    # Account_Name
    if "Account_Name" in record:
        _set("account_name", record.get("Account_Name"))

    # resto de campos
    for k, v in record.items():
        if k in ("id", "Owner", "Account_Name", "Created_Time", "Modified_Time"):
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

        # dict generico
        if isinstance(v, dict):
            if base_col in existing_cols:
                _set(base_col, v)
            continue

        # primitivos
        _set(base_col, v)

    # raw_json completo
    if "raw_json" in existing_cols:
        _set("raw_json", record)

    # default FALSE para booleanos omitidos
    for col, t in existing_types.items():
        if col not in row and t in ("bool", "boolean"):
            row[col] = False

    return row

# ---------------- Upsert ----------------

def chunked(seq, n):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_rows(conn, rows: List[Dict[str, Any]], existing_cols: set):
    if not rows:
        return

    all_keys = set()
    for r in rows:
        all_keys |= set(r.keys())
    cols = [c for c in sorted(all_keys) if c in existing_cols]
    if not cols:
        return

    cols_sql = ",".join(f'"{c}"' for c in cols)
    update_assign = ",".join(f'"{c}"=EXCLUDED."{c}"' for c in cols if c != "zoho_id")
    if 'synced_at' in existing_cols:
        update_assign += ", \"synced_at\"=now()"

    with conn.cursor() as cur:
        for batch in chunked(rows, UPSERT_CHUNK):
            values = [[r.get(c) for c in cols] for r in batch]
            pgx.execute_values(
                cur,
                f'INSERT INTO {TABLE_NAME} ({cols_sql}) VALUES %s '
                f'ON CONFLICT ("zoho_id") DO UPDATE SET {update_assign}',
                values
            )
            if 'synced_at' in existing_cols:
                zoho_ids = [r.get('zoho_id') for r in batch if r.get('zoho_id')]
                if zoho_ids:
                    cur.execute(
                        f'UPDATE {TABLE_NAME} SET "synced_at"=now() '
                        f'WHERE "synced_at" IS NULL AND "zoho_id" = ANY(%s)', (zoho_ids,)
                    )
    conn.commit()

# ---------------- Main ----------------

def main():
    env = load_env()
    token = get_access_token(env)

    with pg_connect(env) as conn:
        conn.autocommit = False
        ensure_state_table(conn)

        since_iso = read_cursor_default_epoch(conn)
        print(f"Iniciando incremental Accounts desde: {since_iso}")

        existing_types = get_existing_columns(conn)
        existing_cols = set(existing_types.keys())

        page_token = None
        total_ids = 0
        ids_buffer: List[str] = []
        max_modified_seen = since_iso

        while True:
            data, info = fetch_page(env, token, since_iso, page_token)
            if not data:
                break

            for rec in data:
                rid = str(rec.get("id")) if rec.get("id") else None
                if rid:
                    ids_buffer.append(rid)
                    total_ids += 1
                mtime = rec.get("Modified_Time")
                if mtime and (not max_modified_seen or mtime > max_modified_seen):
                    max_modified_seen = mtime

            # expand y upsert por grupos de IDs
            if len(ids_buffer) >= BULK_IDS_CHUNK or not info.get("more_records"):
                for group in chunked(ids_buffer, BULK_IDS_CHUNK):
                    expanded = fetch_accounts_by_ids_all_fields(env, token, group)
                    rows = [flatten_account(rec, existing_types) for rec in expanded]
                    upsert_rows(conn, rows, existing_cols)
                ids_buffer = []

            page_token = info.get("next_page_token")
            if not page_token and not info.get("more_records"):
                break

        if max_modified_seen and since_iso and max_modified_seen > since_iso:
            save_cursor(conn, max_modified_seen)
            print(f"Cursor actualizado a: {max_modified_seen}")

        print(f"Incremental Accounts terminado. IDs procesados: {total_ids}")

if __name__ == '__main__':
    main()

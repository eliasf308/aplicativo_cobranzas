# -*- coding: utf-8 -*-
"""
Incremental sync de Zoho CRM Contacts -> Postgres (public.crm_contacts)

Robustez agregada:
- Sesión HTTP con retry/backoff (429/5xx + errores de conexión/lectura).
- Auto-refresh del access token si aparece 401 INVALID_TOKEN en mitad de la corrida.
- Refresh proactivo del token cada 50 min.
- Timeouts diferenciados (connect/read).
- Tamaño de lote de IDs configurable (.env ZOHO_BULK_IDS_CHUNK; default 40).
- Connection: close para evitar sockets colgados.

Cursor:
- Usa crm_sync_state(module='Contacts', last_modified). Si no existe, toma MAX(modified_time) de la tabla o 10 años atrás.
"""

from __future__ import annotations
import json
import time
import random
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import psycopg2
import psycopg2.extras as pgx
from dotenv import dotenv_values

TABLE_SCHEMA = "public"
TABLE_BASENAME = "crm_contacts"
TABLE_NAME = f"{TABLE_SCHEMA}.{TABLE_BASENAME}"

PER_PAGE = 200                 # tamaño de página (listado incremental)
UPSERT_CHUNK = 200             # batch de upsert a DB
SESSION_CONNECT_TIMEOUT = 10   # segundos
SESSION_READ_TIMEOUT = 60      # segundos
SAFE_LIST_FIELDS = ["id", "Modified_Time"]
DEF_TIMEZONE = timezone.utc

# ---------------- Utils / Entorno ----------------

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

def build_session() -> requests.Session:
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    s = requests.Session()
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"Connection": "close", "Accept": "application/json"})
    return s

def _oauth_accounts_domain() -> str:
    # Si necesitás .eu/.in, cambiá acá. Para US: accounts.zoho.com
    return "https://accounts.zoho.com"

def get_access_token_raw(env: Dict[str, str], session: requests.Session) -> str:
    url = f"{_oauth_accounts_domain()}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": env["ZOHO_SELF_CLIENT_ID"],
        "client_secret": env["ZOHO_SELF_CLIENT_SECRET"],
        "refresh_token": env["ZOHO_REFRESH_TOKEN"],
    }
    r = session.post(url, data=data, timeout=(SESSION_CONNECT_TIMEOUT, SESSION_READ_TIMEOUT))
    r.raise_for_status()
    return r.json()["access_token"]

class TokenManager:
    """Gestiona token, refresca ante 401 o cada ~50 min."""
    def __init__(self, env: Dict[str,str], session: requests.Session):
        self.env = env
        self.session = session
        self._token: Optional[str] = None
        self._issued_at: Optional[float] = None

    def refresh(self) -> str:
        self._token = get_access_token_raw(self.env, self.session)
        self._issued_at = time.time()
        print("[INFO] Access token refrescado.")
        return self._token

    def get(self) -> str:
        if not self._token or not self._issued_at:
            return self.refresh()
        # refresh proactivo a los 50 min
        if (time.time() - self._issued_at) > 50 * 60:
            return self.refresh()
        return self._token

# Helper: GET con auto-refresh si 401 INVALID_TOKEN
def zoho_get(url: str, params: Dict[str,Any], tm: TokenManager, session: requests.Session,
             timeout=(SESSION_CONNECT_TIMEOUT, SESSION_READ_TIMEOUT), extra_headers: Optional[Dict[str,str]]=None,
             retry_on_401: int = 1) -> requests.Response:
    for attempt in range(retry_on_401 + 1):
        headers = {"Authorization": f"Zoho-oauthtoken {tm.get()}", "Accept": "application/json"}
        if extra_headers: headers.update(extra_headers)
        r = session.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code == 401:
            code = None
            try:
                js = r.json()
                code = js.get("code")
            except Exception:
                pass
            body_lower = (r.text or "").lower()
            if code in ("INVALID_TOKEN", "AUTHENTICATION_FAILURE") or "invalid oauth token" in body_lower:
                print("[WARN] 401 INVALID_TOKEN detectado. Intentando refresh de token...")
                tm.refresh()
                if attempt < retry_on_401:
                    continue
        r.raise_for_status()
        return r
    return r

# ---------------- DB helpers ----------------

def pg_connect(env):
    return psycopg2.connect(
        host=env["DB_HOST"], port=int(env["DB_PORT"]), dbname=env["DB_NAME"],
        user=env["DB_USER"], password=env["DB_PASSWORD"], connect_timeout=10,
    )

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

def get_cursor(conn) -> Optional[datetime]:
    with conn.cursor() as cur:
        cur.execute("SELECT last_modified FROM public.crm_sync_state WHERE module='Contacts'")
        r = cur.fetchone()
        if r and r[0]:
            dt = r[0]
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=DEF_TIMEZONE)
            return dt
    return None

def set_cursor(conn, new_dt: datetime):
    if new_dt.tzinfo is None:
        new_dt = new_dt.replace(tzinfo=DEF_TIMEZONE)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.crm_sync_state (module, last_modified, updated_at)
            VALUES ('Contacts', %s, now())
            ON CONFLICT (module) DO UPDATE
            SET last_modified=EXCLUDED.last_modified,
                updated_at=now()
            """,
            (new_dt,)
        )
    conn.commit()

# ---------------- Zoho helpers ----------------

_FIELDS_CACHE: Optional[List[str]] = None

def get_contacts_api_fields(env, tm: TokenManager, session: requests.Session) -> List[str]:
    global _FIELDS_CACHE
    if _FIELDS_CACHE is not None:
        return _FIELDS_CACHE
    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/settings/fields"
    params = {"module": "Contacts"}
    r = zoho_get(url, params, tm, session)
    j = r.json() or {}
    _FIELDS_CACHE = [f.get("api_name") for f in j.get("fields", []) if f.get("api_name")]
    return _FIELDS_CACHE

def fetch_page_ids_since(env, tm: TokenManager, since_iso: str, page_token: Optional[str], session: requests.Session):
    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/Contacts"
    params = {
        "fields": ",".join(SAFE_LIST_FIELDS),
        "criteria": f"(Modified_Time:after:{since_iso})",
        "per_page": PER_PAGE
    }
    if page_token:
        params["page_token"] = page_token
    r = zoho_get(url, params, tm, session)
    if r.status_code == 204:
        return [], None, False
    js = r.json() or {}
    data = js.get("data") or []
    info = js.get("info") or {}
    more = bool(info.get("more_records"))
    next_token = info.get("next_page_token") or None
    return data, next_token, more

def fetch_by_ids_all_fields(env, tm: TokenManager, ids: List[str], session: requests.Session) -> List[Dict[str, Any]]:
    if not ids:
        return []

    fields_all = get_contacts_api_fields(env, tm, session) or []
    base_extra = ["Owner", "Full_Name", "Last_Name", "Email", "Created_Time", "Modified_Time", "Account_Name"]

    def dedup(seq):
        seen, out = set(), []
        for x in seq:
            if x and x not in seen:
                seen.add(x); out.append(x)
        return out

    fields_no_base = [f for f in fields_all if f not in ("id",) + tuple(base_extra)]

    # chunks de fields (máx 50 por request, incluido "id")
    chunks: List[List[str]] = []
    first_room = 50 - 1 - len(base_extra)
    first_chunk_rest = fields_no_base[:max(0, first_room)]
    chunks.append(dedup(["id"] + base_extra + first_chunk_rest))
    idx = len(first_chunk_rest)
    while idx < len(fields_no_base):
        chunk_fields = ["id"] + fields_no_base[idx: idx + 49]
        chunks.append(dedup(chunk_fields))
        idx += 49

    url = f"{env['ZOHO_API_DOMAIN']}/crm/v5/Contacts"
    merged: Dict[str, Dict[str, Any]] = {}

    for fields in chunks:
        params = {"ids": ",".join(ids), "fields": ",".join(fields)}

        last_exc = None
        for attempt in range(1, 4):
            try:
                # zoho_get maneja 401 con refresh automático
                r = zoho_get(url, params, tm, session)
                js = r.json()
                data = js.get("data") or []
                for rec in data:
                    rid = str(rec.get("id")) if rec else None
                    if not rid:
                        continue
                    if rid not in merged:
                        merged[rid] = rec
                    else:
                        for k, v in rec.items():
                            if v is not None and (k not in merged[rid] or merged[rid][k] in (None, "", [])):
                                merged[rid][k] = v
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ReadTimeout) as e:
                last_exc = e
                sleep = round(1.5 ** attempt + random.uniform(0, 1.5), 2)
                print(f"[WARN] Grupo IDs {ids[0]}.. reintento {attempt}/3 por {e}. Esperando {sleep}s...")
                time.sleep(sleep)
        else:
            raise last_exc

    return list(merged.values())

# ---------------- Flatten dinámico ----------------

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

def norm_col(name: str) -> str:
    s = name.replace(" ", "_").replace("/", "_").replace("-", "_")
    s = s.replace("(", "").replace(")", "").replace(".", "").replace("%", "")
    while "__" in s:
        s = s.replace("__", "_")
    return s.lower()

def flatten_contact(record: Dict[str, Any], existing_types: Dict[str, str]) -> Dict[str, Any]:
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

    # Account_Name (lookup)
    acc = record.get("Account_Name")
    if isinstance(acc, dict):
        _set("account_id", acc.get("id"))
        _set("account_name", acc.get("name"))
    elif isinstance(acc, str):
        _set("account_name", acc)

    # Full_Name / Last_Name / Email
    if "Full_Name" in record:
        _set("full_name", record.get("Full_Name"))
    if "Last_Name" in record:
        _set("last_name", record.get("Last_Name"))
    if "Email" in record:
        _set("email", record.get("Email"))

    # resto de fields
    for k, v in record.items():
        if k in ("id", "Owner", "Account_Name", "Full_Name", "Last_Name", "Email", "Created_Time", "Modified_Time"):
            continue
        base_col = norm_col(k)

        if isinstance(v, dict) and ("id" in v or "name" in v or "email" in v):
            _set(f"{base_col}_id", v.get("id"))
            _set(f"{base_col}_name", v.get("name"))
            _set(f"{base_col}_email", v.get("email"))
            if base_col in existing_cols and base_col not in row:
                _set(base_col, v.get("name") or v.get("id"))
            continue

        if isinstance(v, list):
            if base_col in existing_cols:
                _set(base_col, v)
            if v and isinstance(v[0], dict):
                _set(f"{base_col}_ids", "|".join([str(x.get("id") or "") for x in v]))
                _set(f"{base_col}_names", "|".join([str(x.get("name") or "") for x in v]))
            continue

        if isinstance(v, dict):
            if base_col in existing_cols:
                _set(base_col, v)
            continue

        _set(base_col, v)

    if "raw_json" in existing_cols:
        _set("raw_json", record)

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
        update_assign += ', "synced_at"=now()'

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
    session = build_session()
    tm = TokenManager(env, session)  # gestiona token con auto-refresh

    with pg_connect(env) as conn:
        conn.autocommit = False
        existing_types = get_existing_columns(conn)
        existing_cols = set(existing_types.keys())

        cursor_dt = get_cursor(conn)
        if cursor_dt is None:
            with conn.cursor() as cur:
                cur.execute(f"SELECT max(modified_time) FROM {TABLE_NAME}")
                r = cur.fetchone()
                cursor_dt = r[0].replace(tzinfo=DEF_TIMEZONE) if r and r[0] else datetime.now(DEF_TIMEZONE) - timedelta(days=3650)

        start_dt = (cursor_dt - timedelta(minutes=5))
        since_iso = start_dt.astimezone().isoformat(timespec="seconds")
        print(f"Iniciando incremental Contacts desde: {since_iso}")

        try:
            bulk_ids_chunk = int(env.get("ZOHO_BULK_IDS_CHUNK", "40"))
            if bulk_ids_chunk < 10 or bulk_ids_chunk > 100:
                bulk_ids_chunk = 40
        except Exception:
            bulk_ids_chunk = 40

        total = 0
        page_token = None
        more = True
        max_seen_dt: Optional[datetime] = cursor_dt

        while more:
            page, next_token, more = fetch_page_ids_since(env, tm, since_iso, page_token, session)
            page_token = next_token
            if not page:
                break

            ids = []
            for rec in page:
                rid = rec.get("id")
                if rid:
                    ids.append(str(rid))
                mt = rec.get("Modified_Time")
                if mt:
                    try:
                        dt = datetime.fromisoformat(mt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=DEF_TIMEZONE)
                    except Exception:
                        dt = None
                    if dt and (max_seen_dt is None or dt > max_seen_dt):
                        max_seen_dt = dt

            for group in chunked(ids, bulk_ids_chunk):
                expanded = fetch_by_ids_all_fields(env, tm, group, session)
                rows = [flatten_contact(rec, existing_types) for rec in expanded]
                upsert_rows(conn, rows, existing_cols)
                total += len(rows)
                print(f"Contacts lote: {len(rows)} (acumulado: {total})")

        if max_seen_dt:
            set_cursor(conn, max_seen_dt)
            print(f"Contacts cursor actualizado a: {max_seen_dt.isoformat(timespec='seconds')}")

        print(f"Contacts incremental terminado. Registros procesados: {total}")

if __name__ == '__main__':
    main()

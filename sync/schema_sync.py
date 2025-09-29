# sync/schema_sync.py
# Sincroniza el ESQUEMA (CREATE/ALTER TABLE) de Postgres con TODOS los campos de Zoho CRM
# para los módulos: Deals, Accounts, Contacts.
#
# - Se conecta a Zoho con tu REFRESH_TOKEN (.env)
# - Llama /crm/v5/settings/fields?module=<...> para traer TODA la metadata
# - Crea/actualiza tablas: crm_deals, crm_accounts, crm_contacts
# - Mapea tipos Zoho -> Postgres
# - Para lookups (lookup/ownerlookup) crea columnas <campo>_id (BIGINT) y <campo>_name (TEXT)
# - Para multiselectlookup usa JSONB
# - No borra columnas existentes; solo agrega las que falten. Es idempotente.

import os
import re
import json
from typing import Dict, List, Tuple

import requests
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# -------- Config desde .env --------
ZOHO_DC = os.getenv("ZOHO_DC", "us")
ZOHO_API_DOMAIN = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com")
ZOHO_CLIENT_ID = os.getenv("ZOHO_SELF_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_SELF_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

DB_NAME = os.getenv("DB_NAME", "cobranzas_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

MODULES = {
    "Deals": "crm_deals",
    "Accounts": "crm_accounts",
    "Contacts": "crm_contacts",
}

# --- Helpers ---

def get_access_token() -> str:
    """Intercambia el refresh_token por un access_token."""
    url = f"https://accounts.zoho.com/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "refresh_token": ZOHO_REFRESH_TOKEN,
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "access_token" not in j:
        raise RuntimeError(f"Zoho no devolvió access_token: {j}")
    return j["access_token"]

def fetch_fields(module_api_name: str, token: str) -> List[Dict]:
    """Trae todos los fields del módulo vía /settings/fields."""
    url = f"{ZOHO_API_DOMAIN}/crm/v5/settings/fields"
    params = {"module": module_api_name}
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    # No hago raise_for_status para mostrar json de error si lo hubiera
    try:
        j = r.json()
    except Exception:
        raise RuntimeError(f"Respuesta no JSON ({r.status_code}): {r.text[:500]}")
    if "fields" not in j:
        raise RuntimeError(f"Zoho devolvió sin 'fields': {json.dumps(j)[:500]}")
    return j["fields"]

_name_cache = set()
def norm_col(name: str) -> str:
    """Normaliza api_name de Zoho a snake_case seguro para Postgres."""
    # Reemplazo no alfanumérico por _
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    if not s:
        s = "field"
    # Evitar colisiones
    base = s
    i = 2
    while s in _name_cache:
        s = f"{base}_{i}"
        i += 1
    _name_cache.add(s)
    return s

def sql_type_for_field(f: Dict) -> Tuple[str, Dict]:
    """
    Mapea data_type de Zoho a Postgres.
    Devuelve (tipo_sql, extras) donde extras puede contener hints como {"lookup": True}
    """
    dt = (f.get("data_type") or "").lower()
    extras = {}
    # Tipos básicos
    if dt in ("text", "textarea", "email", "phone", "website", "profileimage", "picklist"):
        return "TEXT", extras
    if dt in ("integer",):
        return "INTEGER", extras
    if dt in ("double", "percent"):
        return "DOUBLE PRECISION", extras
    if dt in ("currency",):
        return "NUMERIC(18,2)", extras
    if dt in ("boolean",):
        return "BOOLEAN", extras
    if dt in ("date",):
        return "DATE", extras
    if dt in ("datetime",):
        return "TIMESTAMPTZ", extras
    if dt in ("bigint",):
        return "BIGINT", extras
    # Multiselect picklist (si aparece)
    if dt in ("multiselectpicklist",):
        return "TEXT[]", extras
    # Lookups
    if dt in ("lookup", "ownerlookup", "userlookup"):
        extras["lookup"] = True
        return "LOOKUP", extras   # marcador
    if dt in ("multiselectlookup", "subform"):
        extras["json"] = True
        return "JSONB", extras
    if dt in ("layout",):
        # Metadata de layout: lo guardo como JSONB
        extras["json"] = True
        return "JSONB", extras
    if dt in ("formula",):
        # Si Zoho da el tipo de retorno, lo uso; si no, TEXT
        rt = (f.get("formula_return_type") or "").lower()
        if rt in ("integer",):
            return "INTEGER", extras
        if rt in ("double", "percent"):
            return "DOUBLE PRECISION", extras
        if rt in ("currency",):
            return "NUMERIC(18,2)", extras
        if rt in ("boolean",):
            return "BOOLEAN", extras
        if rt in ("date",):
            return "DATE", extras
        if rt in ("datetime",):
            return "TIMESTAMPTZ", extras
        return "TEXT", extras
    # Fallback
    extras["json"] = True
    return "JSONB", extras

def ensure_table(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id BIGINT PRIMARY KEY,
                created_time TIMESTAMPTZ,
                modified_time TIMESTAMPTZ
            );
        """)
    conn.commit()

def get_existing_columns(conn, table: str) -> Dict[str, str]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table.split(".")[-1],))
        cols = {}
        for row in cur.fetchall():
            cols[row["column_name"]] = row["data_type"] or row["udt_name"]
        return cols

def add_column(cur, table: str, col: str, sqltype: str):
    cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{col}" {sqltype};')

def sync_module(conn, module_api: str, table: str, fields: List[Dict]) -> List[Tuple[str, str]]:
    """
    Crea/actualiza columnas para un módulo.
    Devuelve lista de (columna_creada, tipo) agregadas.
    """
    ensure_table(conn, table)
    existing = get_existing_columns(conn, table)

    created = []

    # Reset cache para evitar colisiones entre módulos
    global _name_cache
    _name_cache = set(existing.keys())

    with conn.cursor() as cur:
        for f in fields:
            api_name = f.get("api_name")
            if not api_name:
                continue

            col = norm_col(api_name)
            sqltype, extras = sql_type_for_field(f)

            # Lookups: creo <campo>_id BIGINT y <campo>_name TEXT
            if extras.get("lookup"):
                id_col = norm_col(f"{api_name}_id")
                name_col = norm_col(f"{api_name}_name")
                if id_col not in existing:
                    add_column(cur, table, id_col, "BIGINT")
                    created.append((id_col, "BIGINT"))
                    existing[id_col] = "bigint"
                if name_col not in existing:
                    add_column(cur, table, name_col, "TEXT")
                    created.append((name_col, "TEXT"))
                    existing[name_col] = "text"
                # (Opcional) email para ownerlookup: si querés
                continue

            # Normal y JSONB
            if col not in existing:
                if sqltype == "LOOKUP":
                    # por si acaso (no debería entrar acá)
                    add_column(cur, table, f"{col}_id", "BIGINT")
                    add_column(cur, table, f"{col}_name", "TEXT")
                    created.append((f"{col}_id", "BIGINT"))
                    created.append((f"{col}_name", "TEXT"))
                    existing[f"{col}_id"] = "bigint"
                    existing[f"{col}_name"] = "text"
                else:
                    add_column(cur, table, col, sqltype)
                    created.append((col, sqltype))
                    existing[col] = sqltype.lower()

    conn.commit()
    return created

def main():
    print("Obteniendo access_token de Zoho…")
    token = get_access_token()
    print("OK.")

    # Conexión DB
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )

    summary = {}
    for module_api, table in MODULES.items():
        print(f"\n==> Módulo: {module_api}  -> Tabla: {table}")
        fields = fetch_fields(module_api, token)
        created = sync_module(conn, module_api, table, fields)
        summary[module_api] = created
        if created:
            for c, t in created:
                print(f"  + columna creada: {c} ({t})")
        else:
            print("  (Sin cambios)")

    conn.close()

    print("\nResumen:")
    for m, cols in summary.items():
        print(f"  {m}: {len(cols)} columnas nuevas")

if __name__ == "__main__":
    main()

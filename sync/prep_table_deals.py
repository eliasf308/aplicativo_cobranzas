# sync/prep_table_deals.py
# Crea/actualiza la tabla public.crm_deals para que tenga TODAS las columnas
# del módulo Deals en Zoho (incluye aplanado de lookups: _id, _name, _email).

import os
import requests
import psycopg2

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(ROOT_DIR, ".env")

MODULE_API = "Deals"
TABLE_NAME = "crm_deals"

EXCLUDE_API_NAMES = {"Layout"}  # No aporta valor en destino

# Tipos Zoho -> tipos Postgres (por defecto TEXT si no mapeamos algo)
TYPE_MAP = {
    "text": "TEXT",
    "textarea": "TEXT",
    "email": "TEXT",
    "phone": "TEXT",
    "url": "TEXT",
    "picklist": "TEXT",
    "multiselectpicklist": "TEXT",
    "integer": "INTEGER",
    "longinteger": "INTEGER",
    "double": "DOUBLE PRECISION",
    "percent": "DOUBLE PRECISION",
    "bigint": "BIGINT",
    "currency": "NUMERIC(18,2)",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMPTZ",
    "formula": "TEXT",          # el resultado puede variar; guardamos texto
    "ownerlookup": "LOOKUP",    # se aplanan abajo
    "lookup": "LOOKUP",         # se aplanan abajo
}

def load_env(path):
    env = {}
    if os.path.exists(path):
        for line in open(path, "r", encoding="utf-8"):
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env

def get_access_token(env):
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": env["ZOHO_SELF_CLIENT_ID"],
        "client_secret": env["ZOHO_SELF_CLIENT_SECRET"],
        "refresh_token": env["ZOHO_REFRESH_TOKEN"],
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    j = r.json()
    if "access_token" not in j:
        raise RuntimeError(f"Zoho no devolvió access_token: {j}")
    return j["access_token"], j.get("api_domain", "https://www.zohoapis.com")

def fetch_fields(api_domain, token, module_api):
    url = f"{api_domain}/crm/v5/settings/fields?module={module_api}"
    h = {"Authorization": f"Zoho-oauthtoken {token}"}
    r = requests.get(url, headers=h, timeout=60)
    r.raise_for_status()
    j = r.json()
    return j.get("fields", [])

def pg_conn(env):
    return psycopg2.connect(
        dbname=env.get("DB_NAME", "cobranzas_db"),
        user=env.get("DB_USER", "postgres"),
        password=env.get("DB_PASSWORD", ""),
        host=env.get("DB_HOST", "localhost"),
        port=int(env.get("DB_PORT", "5432")),
    )

def table_exists(cur, table):
    cur.execute("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='public' AND table_name=%s
        )
    """, (table,))
    return cur.fetchone()[0]

def get_existing_columns(cur, table):
    cur.execute("""
        SELECT lower(column_name)
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
    """, (table,))
    return {row[0] for row in cur.fetchall()}

def main():
    env = load_env(ENV_PATH)
    token, api_domain = get_access_token(env)

    print("Pidiendo definición de campos de Zoho (Deals)...")
    fields = fetch_fields(api_domain, token, MODULE_API)

    # Calculamos el set de columnas requeridas en Postgres (en minúsculas)
    required_cols = {
        "id": "BIGINT",
        "created_time": "TIMESTAMPTZ",
        "modified_time": "TIMESTAMPTZ",
    }

    for f in fields:
        api_name = f.get("api_name")
        if not api_name or api_name in EXCLUDE_API_NAMES:
            continue
        dt = (f.get("data_type") or "").lower()
        visible = f.get("visible", True)
        if not visible:
            # evitamos columnas ocultas; si luego las necesitás, se puede incluir
            continue

        col = api_name.lower()
        if TYPE_MAP.get(dt) == "LOOKUP":
            # Aplanamos: <campo>_id, _name, _email
            required_cols[f"{col}_id"] = "TEXT"
            required_cols[f"{col}_name"] = "TEXT"
            required_cols[f"{col}_email"] = "TEXT"
        else:
            pg_type = TYPE_MAP.get(dt, "TEXT")
            required_cols[col] = pg_type

    with pg_conn(env) as conn:
        with conn.cursor() as cur:
            if not table_exists(cur, TABLE_NAME):
                print(f"Creando tabla public.{TABLE_NAME} ...")
                cur.execute(f"""
                    CREATE TABLE public.{TABLE_NAME} (
                        id BIGINT PRIMARY KEY,
                        created_time TIMESTAMPTZ,
                        modified_time TIMESTAMPTZ
                    );
                """)
                conn.commit()

            existing = get_existing_columns(cur, TABLE_NAME)
            to_add = [(c, t) for c, t in required_cols.items() if c not in existing]

            if not to_add:
                print("No hay columnas nuevas que agregar. La tabla ya está alineada.")
            else:
                print(f"Agregando {len(to_add)} columna(s) a public.{TABLE_NAME} ...")
                for col, typ in to_add:
                    cur.execute(f'ALTER TABLE public.{TABLE_NAME} ADD COLUMN "{col}" {typ};')
                conn.commit()
                for col, typ in to_add:
                    print(f"  + {col} {typ}")

    print("Listo. Tabla sincronizada con los campos de Deals.")

if __name__ == "__main__":
    main()

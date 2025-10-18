# sync/enrich_deals_json.py
# Enriquece public.crm_deals rellenando raw_json con el JSON completo de Zoho.
# Modo "missing": solo filas con raw_json IS NULL
# Modo "all":     todas las deals (útil para refrescar masivamente)
#
# Uso:
#   python -u sync\enrich_deals_json.py --mode missing
#   python -u sync\enrich_deals_json.py --mode all

import os
import json
import time
from typing import List, Dict, Tuple

import requests
import psycopg2
import psycopg2.extras as pgx
from dotenv import dotenv_values

# ==========================
# Config
# ==========================
PER_PAGE_IDS   = 100     # Zoho permite ~100 ids por llamada
SESSION_TIMEOUT = 60
SLEEP_BETWEEN   = 0.20   # respiro entre lotes (segundos)
MAX_RETRIES     = 4      # reintentos red/5xx
BACKOFF_BASE    = 1.8    # backoff exponencial


# ==========================
# Helpers
# ==========================
def load_env() -> Dict[str, str]:
    # .env está un nivel arriba de /sync
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    env = dotenv_values(env_path)
    required = [
        "ZOHO_API_DOMAIN",
        "ZOHO_SELF_CLIENT_ID",
        "ZOHO_SELF_CLIENT_SECRET",
        "ZOHO_REFRESH_TOKEN",
        "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD",
    ]
    missing = [k for k in required if not env.get(k)]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {', '.join(missing)}")
    return env


def get_access_token(env: Dict[str, str]) -> str:
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": env["ZOHO_SELF_CLIENT_ID"],
        "client_secret": env["ZOHO_SELF_CLIENT_SECRET"],
        "refresh_token": env["ZOHO_REFRESH_TOKEN"],
    }
    r = requests.post(url, data=data, timeout=SESSION_TIMEOUT)
    try:
        js = r.json()
    except Exception:
        js = {"raw": r.text}
    if r.status_code != 200 or "access_token" not in js:
        raise RuntimeError(f"Error al renovar token: {js}")
    return js["access_token"]


def get_db_conn(env: Dict[str, str]):
    return psycopg2.connect(
        host=env["DB_HOST"],
        port=int(env["DB_PORT"]),
        dbname=env["DB_NAME"],
        user=env["DB_USER"],
        password=env["DB_PASSWORD"],
        connect_timeout=10,
    )


def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.request(method, url, timeout=SESSION_TIMEOUT, **kwargs)
            if r.status_code >= 400:
                print(f"[Zoho {r.status_code}] {r.text[:800]}")
            r.raise_for_status()
            return r
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == MAX_RETRIES:
                raise
            sleep_s = BACKOFF_BASE ** attempt
            print(f"[WARN] Error de red/timeout ({e}). Reintento {attempt}/{MAX_RETRIES} en {sleep_s:.1f}s...")
            time.sleep(sleep_s)
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", 0) or 0
            if code in (429, 500, 502, 503, 504) and attempt < MAX_RETRIES:
                sleep_s = BACKOFF_BASE ** attempt
                print(f"[WARN] HTTP {code}. Reintento {attempt}/{MAX_RETRIES} en {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue
            raise


def fetch_deals_by_ids(env: Dict[str, str], token: str, ids: List[str]) -> List[Dict]:
    """Trae JSON completo de Deals para un lote de IDs (sin 'fields' para no recortar)."""
    if not ids:
        return []
    url = f'{env["ZOHO_API_DOMAIN"]}/crm/v5/Deals'
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {"ids": ",".join(ids)}
    try:
        r = request_with_retry("GET", url, headers=headers, params=params)
        js = r.json()
        return js.get("data", []) or []
    except Exception as e:
        # log suave y seguimos con los siguientes lotes
        print(f"[WARN] Error al pedir ids={ids[:3]}... ({len(ids)} ids). Detalle: {e}")
        return []


def update_raw_json(cur, rows: List[Dict]) -> int:
    """
    Actualiza raw_json (jsonb) y synced_at para los zoho_id que vengan en rows.
    rows: lista de dicts con al menos {"id": "...", ...}
    """
    vals: List[Tuple[str, str]] = []
    for rec in rows:
        zid = str(rec.get("id") or "")
        if not zid:
            continue
        # Guardamos el JSON como string; en SQL lo casteamos a jsonb
        vals.append((zid, json.dumps(rec, ensure_ascii=False)))
    if not vals:
        return 0

    # ⚠️ Parche clave: castear a jsonb para evitar "type text vs jsonb"
    sql = """
        WITH v(zoho_id, raw_json) AS (VALUES %s)
        UPDATE public.crm_deals d
        SET raw_json = v.raw_json::jsonb,
            synced_at = now()
        FROM v
        WHERE d.zoho_id = v.zoho_id
    """
    pgx.execute_values(cur, sql, vals, page_size=1000)
    return len(vals)


def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# ==========================
# Main
# ==========================
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode",
        choices=["missing", "all"],
        default="missing",
        help="missing: solo filas con raw_json IS NULL; all: todas las deals",
    )
    args = ap.parse_args()

    env = load_env()
    token = get_access_token(env)

    with get_db_conn(env) as conn:
        conn.autocommit = False
        cur = conn.cursor()

        if args.mode == "missing":
            cur.execute("SELECT zoho_id FROM public.crm_deals WHERE raw_json IS NULL AND zoho_id IS NOT NULL")
        else:
            cur.execute("SELECT zoho_id FROM public.crm_deals WHERE zoho_id IS NOT NULL")

        ids = [r[0] for r in cur.fetchall()]
        # Por las dudas, eliminamos duplicados preservando orden:
        seen = set()
        ids = [x for x in ids if not (x in seen or seen.add(x))]

        total = len(ids)
        print(f"Modo: {args.mode}. IDs a procesar: {total}")

        processed = 0
        for lot in chunked(ids, PER_PAGE_IDS):
            data = fetch_deals_by_ids(env, token, lot)
            updated = update_raw_json(cur, data)
            conn.commit()
            processed += len(lot)
            print(f"Lote {processed}/{total} (actualizados {updated})")
            time.sleep(SLEEP_BETWEEN)

        print("Enriquecimiento completado.")


if __name__ == "__main__":
    main()

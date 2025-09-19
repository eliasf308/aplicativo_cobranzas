# sync_accounts_min.py
import os
import json
import requests
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv()  # lee .env

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

def get_access_token():
    url = f"{ACCOUNTS_BASE}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": SELF_ID,
        "client_secret": SELF_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }
    r = requests.post(url, data=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if "access_token" not in js:
        raise RuntimeError(f"Error al renovar token: {js}")
    return js["access_token"]

def fetch_accounts(access_token):
    """Trae hasta 200 Accounts (para validar el flujo)."""
    url = f"{API_BASE}/crm/v5/Accounts"
    params = {
        "per_page": 200,
        "fields": "id,Account_Name,Owner,Created_Time,Modified_Time",
    }
    headers = {"Authorization": f"Zoho-oauthtoken {access_token}"}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])

def upsert_accounts(rows):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
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
    with conn:
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
    conn.close()

if __name__ == "__main__":
    token = get_access_token()
    accounts = fetch_accounts(token)
    upsert_accounts(accounts)
    print(f"Procesadas {len(accounts)} cuentas (upsert).")

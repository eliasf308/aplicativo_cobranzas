import os, psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
)
with conn, conn.cursor() as cur:
    print("search_path actual:")
    cur.execute("SHOW search_path;")
    print(cur.fetchone()[0], "\n")

    print("Tablas llamadas crm_deals (todas los schemas):")
    cur.execute("""
        SELECT n.nspname AS schema, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND c.relname = 'crm_deals'
        ORDER BY 1,2;
    """)
    for row in cur.fetchall():
        print(f" - {row[0]}.{row[1]}")
    print()

    print("Constraints PK/UNIQUE sobre public.crm_deals:")
    cur.execute("""
        SELECT conname, contype
        FROM pg_constraint
        WHERE conrelid = 'public.crm_deals'::regclass
          AND contype IN ('p','u');
    """)
    cons = cur.fetchall()
    if not cons:
        print(" (no hay PK/UNIQUE en public.crm_deals)")
    else:
        for c in cons:
            print(f" - {c[0]} (tipo {c[1]})")

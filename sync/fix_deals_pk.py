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
conn.autocommit = True

with conn.cursor() as cur:
    # Aseguro que la tabla exista al menos con id
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.crm_deals (
            id BIGINT
        );
    """)

    # Paso id a BIGINT (si todavía fuera TEXT), y a NOT NULL
    cur.execute("""
        DO $$
        BEGIN
            -- si no es bigint, intento castear
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='crm_deals'
                  AND column_name='id' AND data_type <> 'bigint'
            ) THEN
                BEGIN
                    ALTER TABLE public.crm_deals
                    ALTER COLUMN id TYPE BIGINT USING NULLIF(id::text,'')::bigint;
                EXCEPTION WHEN others THEN
                    RAISE NOTICE 'No pude castear id a BIGINT. Revisar si hay valores no numéricos.';
                END;
            END IF;

            -- NOT NULL
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='crm_deals'
                  AND column_name='id' AND is_nullable = 'YES'
            ) THEN
                BEGIN
                    ALTER TABLE public.crm_deals
                    ALTER COLUMN id SET NOT NULL;
                EXCEPTION WHEN others THEN
                    RAISE NOTICE 'No pude setear NOT NULL en id. ¿Hay filas con id nulo?';
                END;
            END IF;
        END$$;
    """)

    # Creo PK si no existe (idempotente)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = 'public.crm_deals'::regclass
                  AND contype = 'p'
            ) THEN
                BEGIN
                    ALTER TABLE public.crm_deals
                    ADD CONSTRAINT crm_deals_pkey PRIMARY KEY (id);
                EXCEPTION WHEN duplicate_table THEN
                    NULL;
                END;
            END IF;
        END$$;
    """)

print("OK: primary key en public.crm_deals(id) verificada/creada.")

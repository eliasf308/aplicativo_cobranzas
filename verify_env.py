# verify_env.py
import importlib

CHECKS = [
    ("django", "get_version"),
    ("celery", "__version__"),
    ("redis", "__version__"),
    ("psycopg2", "__version__"),
    ("pandas", "__version__"),
    ("openpyxl", "__version__"),
    ("xlsxwriter", "__version__"),
    ("weasyprint", "__version__"),
    ("lxml", "__version__"),
    ("gunicorn", "__version__"),
    ("whitenoise", "__version__"),
]

def get_attr(mod, attr):
    # casos especiales
    if mod == "django" and attr == "get_version":
        import django
        return django.get_version()
    m = importlib.import_module(mod)
    return getattr(m, attr, "unknown")

ok = True
print("=== Verificación de entorno (imports + versiones) ===")
for mod, attr in CHECKS:
    try:
        ver = get_attr(mod, attr)
        print(f"[OK] {mod}: {ver}")
    except Exception as e:
        ok = False
        print(f"[FALTA/ERROR] {mod}: {e.__class__.__name__}: {e}")

# chequeo de módulos críticos de Django
try:
    import django.conf
    import django.core
    print("[OK] django submódulos: conf/core")
except Exception as e:
    ok = False
    print(f"[FALTA/ERROR] django submódulos: {e}")

print("\nResultado:", "TODO OK ✅" if ok else "Hay faltantes/errores ❌")

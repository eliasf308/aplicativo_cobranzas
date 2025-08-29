# art/templatetags/fmt.py
from django import template
from decimal import Decimal, InvalidOperation
from datetime import date, datetime

register = template.Library()

def _to_decimal(value):
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    s = str(value).strip()
    s = s.replace("$", "").replace("ARS", "").replace(" ", "")
    # Si viene en formato AR (100.000,00) -> paso a 100000.00
    if "," in s and "." in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

@register.filter
def ars(value):
    """
    Formatea como moneda argentina: $ 100.000,00
    """
    n = _to_decimal(value)
    if n is None:
        return ""
    s = f"{n:,.2f}"               # 123,456.78
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")  # 123.456,78
    return f"$ {s}"

@register.filter
def ddmmyyyy(value):
    """
    Devuelve fecha en formato dd/mm/aaaa.
    Acepta date/datetime o strings 'YYYY-MM-DD' / 'YYYY/MM/DD' / 'DD/MM/YYYY'.
    """
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.date().strftime("%d/%m/%Y")
    if isinstance(value, date):
        return value.strftime("%d/%m/%Y")
    s = str(value).strip()
    # ya está en dd/mm/yyyy
    if len(s) == 10 and s[2] == "/" and s[5] == "/":
        return s
    # YYYY-MM-DD o YYYY/MM/DD
    for sep in ("-", "/"):
        parts = s.split(sep)
        if len(parts) == 3 and len(parts[0]) == 4:
            try:
                y, m, d = map(int, parts)
                return f"{d:02d}/{m:02d}/{y:04d}"
            except Exception:
                break
    return s  # fallback

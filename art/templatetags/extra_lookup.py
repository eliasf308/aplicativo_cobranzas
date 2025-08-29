# art/templatetags/extra_lookup.py
from django import template
register = template.Library()

@register.filter
def lookup(obj, key):
    """
    Uso en plantilla:
        {{ fila|lookup:"Deuda total" }}
        {{ fila|lookup:"cliente.nombre" }}  # soporta anidado dict/obj
    """
    try:
        current = obj
        for part in str(key).split('.'):
            if isinstance(current, dict):
                if part in current:
                    current = current[part]
                else:
                    # intento case-insensitive en dicts
                    lowered = {str(k).lower(): k for k in current.keys()}
                    real = lowered.get(part.lower())
                    if real is None:
                        return ""
                    current = current[real]
            else:
                # atributo o índice
                if hasattr(current, part):
                    current = getattr(current, part)
                else:
                    try:
                        idx = int(part) if str(part).isdigit() else part
                        current = current[idx]
                    except Exception:
                        return ""
        return "" if current is None else current
    except Exception:
        return ""

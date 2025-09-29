# art/utils/__init__.py
# -*- coding: utf-8 -*-
"""
Re-exports convenientes para los módulos de ART.

• cargar_consolidado  👉  función que lee el Excel histórico
  (implementada en art/services/excel.py).
• leer_maestro, leer_aseguradora, exportar_excel_tabla
  👉  utilidades que siguen viviendo en art/utils/consolidado.py
      y que usa el servicio «consolidar».

Con esto:
    from art.utils import cargar_consolidado
sigue funcionando, pero ahora apunta al directorio
C:\\Users\\Promecor\\Documents\\ART\\Deuda ART Historico.
"""

# --- Excel histórico -------------------------------------------------
from art.services.excel import cargar_consolidado          # noqa: F401

# --- Funciones que aún se usan en art/services/consolidar.py ---------
from .consolidado import (                                  # noqa: F401
    leer_maestro,
    leer_aseguradora,
    exportar_excel_tabla,
)

__all__ = [
    "cargar_consolidado",
    "leer_maestro",
    "leer_aseguradora",
    "exportar_excel_tabla",
]

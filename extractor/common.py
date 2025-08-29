"""
Funciones y utilidades compartidas por todos los parsers
-------------------------------------------------------

* Limpieza y conversión de números (`clean_number`)
* Limpieza de texto (`clean_text`)
* Carga del mapeo Aseguradora ↔ CUIT y Ramo ↔ variantes
  desde «Ramos-aseguradoras.xlsx»
* Detección de duplicados
* Nombre de mes en castellano
"""

from __future__ import annotations

import os
import re
from datetime import datetime

import pandas as pd

# ----------------------------------------------------------------------
# Rutas y archivo de mapeo
# ----------------------------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
MAPPING_XLSX = os.path.join(BASE_DIR, "Ramos-aseguradoras.xlsx")

# ----------------------------------------------------------------------
# Conversión y limpieza de números
# ----------------------------------------------------------------------
def clean_number(raw: str | float | int | None) -> float:
    """
    Convierte un número escrito al estilo latino o anglosajón a float.

    Ejemplos admitidos
    ------------------
    '318.808,19' → 318808.19
    '318,808.19' → 318808.19
    '318808,19'  → 318808.19
    '318808.19'  → 318808.19
    318808       → 318808.0
    """
    if raw is None or (isinstance(raw, str) and raw.strip() == ""):
        raise ValueError("Número vacío")

    # Ya es numérico
    if isinstance(raw, (int, float)):
        return float(raw)

    txt = str(raw).strip()

    if "," in txt and "." in txt:
        # Formato AR habitual
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(".", "").replace(",", ".")
    else:
        txt = txt.replace(",", "")

    return float(txt)

# ----------------------------------------------------------------------
# Limpieza de texto genérica
# ----------------------------------------------------------------------
_CLEAN_RE = re.compile(r"\s+")

def clean_text(text: str) -> str:
    """Quita saltos de línea repetidos / dobles espacios y normaliza."""
    return _CLEAN_RE.sub(" ", text).strip()

# ----------------------------------------------------------------------
# Carga de Aseguradoras ↔ CUIT y Ramo ↔ variantes
# ----------------------------------------------------------------------
def _load_mappings() -> tuple[dict[str, set[str]], dict[str, str]]:
    """
    Devuelve:
      · aseg_to_cuits : {Aseguradora → {cuits}}
      · ramo_map      : {VARIANTE_EN_MAYÚSCULAS → RamoCanónico}
    """
    df = pd.read_excel(MAPPING_XLSX, sheet_name=0)

    aseg_to_cuits: dict[str, set[str]] = {}
    ramo_map: dict[str, str] = {}

    for _, row in df.iterrows():
        # ------------- Aseguradoras / CUIT ----------------------------
        aseg = str(row.get("Aseguradora", "")).strip()
        cuits = str(row.get("CUIT", "")).split(";")

        cuits_norm: set[str] = set()
        for c in cuits:
            c = c.strip()
            if not c:
                continue
            cuits_norm.add(c)                   # versión con guiones
            cuits_norm.add(c.replace("-", ""))  # versión sin guiones

        if aseg:
            aseg_to_cuits.setdefault(aseg, set()).update(cuits_norm)

        # ------------- Ramo y variantes -------------------------------
        for col in row.index:
            if str(col).lower().startswith("ramo"):
                canon_name = (
                    col.split(":", 1)[0].replace("Ramo", "").strip()
                    or str(col).strip()
                )
                variants = (
                    str(row[col]).split("-") if pd.notna(row[col]) else []
                )
                for var in variants:
                    ramo_map[var.strip().upper()] = canon_name

    return aseg_to_cuits, ramo_map


_ASEG_TO_CUITS, _RAMO_MAP = _load_mappings()

# ----------------------------------------------------------------------
# Utilidades de mapeo
# ----------------------------------------------------------------------
def map_ramo(raw: str) -> str:
    """Devuelve el nombre canónico del ramo (si existe) o el original limpio."""
    return _RAMO_MAP.get(raw.upper().strip(), raw.strip())


def aseguradora_desde_cuit(cuit: str) -> str | None:
    """Identifica la aseguradora a partir del CUIT (con o sin guiones)."""
    cuit_norm = cuit.replace("-", "").strip()
    for aseg, cuits in _ASEG_TO_CUITS.items():
        if cuit_norm in {c.replace("-", "") for c in cuits}:
            return aseg
    return None

# ----------------------------------------------------------------------
# Control de duplicados
# ----------------------------------------------------------------------
def is_duplicate(df: pd.DataFrame, row: dict) -> bool:
    """
    Comprueba si la combinación
      (Aseguradora, Ramo, Poliza, Endoso)
    ya existe en el DataFrame.
    """
    mask = (
        (df["Aseguradora"] == row["Aseguradora"])
        & (df["Ramo"] == row["Ramo"])
        & (df["Poliza"] == row["Poliza"])
        & (df["Endoso"] == row["Endoso"])
    )
    return mask.any()

# ----------------------------------------------------------------------
# Fechas
# ----------------------------------------------------------------------
def month_name_es(mes: int) -> str:
    """Devuelve el nombre del mes en castellano (1-12)."""
    nombres = [
        "",  # 0
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]
    if not 1 <= mes <= 12:
        raise ValueError("Mes fuera de rango")
    return nombres[mes]

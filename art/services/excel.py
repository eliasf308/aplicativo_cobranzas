# art/services/excel.py
"""
Lee un Consolidado_ART_MM-AAAA[...].xlsx desde el histórico y devuelve
la info agrupada por Email del trato para una hoja dada (p.ej. "Deuda Promecor" o "Productor").
Incluye:
- Búsqueda robusta del archivo (con o sin timestamp en el nombre).
- Soporte de periodo "MM/AAAA" o "MM-AAAA".
- Conversión segura de 'Q periodos deudores' a numérico.
- Umbral de 'intimado' parametrizable (default: 3).
"""

from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd

# Ruta raíz donde viven los excels históricos
HISTORICO_DIR = Path(r"C:\Users\Promecor\Documents\ART\Deuda ART Historico")

# Columnas mínimas requeridas para esta función
REQUIRED_MIN = [
    "Periodo",
    "CUIT",
    "Deuda total",
    "Q periodos deudores",
    "Email del trato",
]

def _normalizar_periodo(periodo: str) -> tuple[str, str]:
    """
    Acepta 'MM/AAAA' o 'MM-AAAA' y devuelve (MM, AAAA)
    """
    p = periodo.strip().replace("-", "/")
    try:
        mm, yyyy = p.split("/")
    except ValueError:
        raise ValueError(f"Periodo inválido: '{periodo}'. Formatos válidos: 'MM/AAAA' o 'MM-AAAA'.")
    mm = mm.zfill(2)
    if len(yyyy) != 4 or not yyyy.isdigit():
        raise ValueError(f"Año inválido en periodo: '{periodo}'")
    return mm, yyyy

def _buscar_consolidado(mm: str, yyyy: str) -> Path:
    """
    Busca el archivo exacto y, si no existe, busca por patrón con timestamp.
    Devuelve el path del archivo más reciente que matchee.
    """
    exacto = HISTORICO_DIR / f"Consolidado_ART_{mm}-{yyyy}.xlsx"
    if exacto.exists():
        return exacto

    candidatos = sorted(
        HISTORICO_DIR.glob(f"Consolidado_ART_{mm}-{yyyy}*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidatos:
        return candidatos[0]

    raise FileNotFoundError(
        f"No se encontró ningún archivo para {mm}-{yyyy} en '{HISTORICO_DIR}'. "
        f"Busqué 'Consolidado_ART_{mm}-{yyyy}.xlsx' y 'Consolidado_ART_{mm}-{yyyy}*.xlsx'."
    )

def _validar_columnas(df: pd.DataFrame) -> None:
    faltan = [c for c in REQUIRED_MIN if c not in df.columns]
    if faltan:
        raise ValueError(f"Faltan columnas mínimas en la hoja: {faltan}")

def cargar_consolidado(
    periodo: str,
    hoja: str,
    umbral_intimado: float = 3.0,
    engine: Optional[str] = "openpyxl",
) -> List[Dict[str, Any]]:
    """
    Lee la hoja pedida y agrupa por 'Email del trato'.

    Args:
        periodo           -> "07/2025" o "07-2025"
        hoja              -> "Deuda Promecor" o "Productor" (u otra hoja válida)
        umbral_intimado   -> valor de Q desde el cual se marca 'intimado' (default 3.0)
        engine            -> engine de pandas para leer excel (default 'openpyxl')

    Returns:
        [
          {
            "email": "cliente@dominio.com",
            "filas": [ {col1:…, col2:…}, … ],   # todas las filas de ese mail
            "intimado": True/False,              # ¿algún contrato con Q >= umbral?
          },
          ...
        ]
    """
    # 1) Periodo → nombre de archivo robusto
    mm, yyyy = _normalizar_periodo(periodo)
    archivo = _buscar_consolidado(mm, yyyy)

    # 2) Leer hoja
    try:
        df = pd.read_excel(archivo, sheet_name=hoja, engine=engine)
    except ValueError as e:
        # pandas lanza ValueError si la hoja no existe
        raise ValueError(f"No se pudo leer la hoja '{hoja}' en '{archivo.name}': {e}")

    if df.empty:
        raise ValueError(f"La hoja '{hoja}' en '{archivo.name}' está vacía.")

    # 3) Validar columnas mínimas
    _validar_columnas(df)

    # 4) Asegurar tipos: Q a numérico (para comparaciones)
    df["Q periodos deudores"] = pd.to_numeric(df["Q periodos deudores"], errors="coerce")

    # 5) Agrupar por email (groupby ignora NaN por defecto)
    resultado: List[Dict[str, Any]] = []
    for email, sub in df.groupby("Email del trato"):
        # sub puede contener NaN en Q; el .ge(umbral) maneja NaN→False
        intimado = sub["Q periodos deudores"].ge(umbral_intimado).any()
        filas = sub.to_dict(orient="records")
        resultado.append({"email": email, "filas": filas, "intimado": bool(intimado)})

    return resultado

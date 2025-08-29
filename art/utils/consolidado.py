# art/utils/consolidado.py
"""
Utilidades para escribir el Consolidado ART y TODAS sus hojas derivadas.

- Parte de un DataFrame base **Consolidado** con columnas:
  Periodo | Razón social | CUIT | Contrato | Aseguradora | Deuda total | Costo mensual
  | Q periodos deudores | Estado contrato | Email del trato | No contactar
  | Productor | Premier | Cliente importante

- Genera las hojas derivadas aplicando la matriz de filtros acordada:
  'No cruzan', 'Sin mail', 'Anuladas', 'No contactar', 'Clientes importantes',
  '1 Q.deudor', 'Premier', 'Productor', 'Deuda Promecor', 'Agregar costo mensual'.

- Formatos:
  CUIT como número (00000000000), Deuda/Costo en ARS "$  #.##0,00", Q con 2 decimales.
  Autotabla por hoja, autofiltro y encabezado congelado.

Versión: 17-AGO-2025
"""

from __future__ import annotations
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

import pandas as pd


# =========================
# Esquema y normalización
# =========================

COLUMNAS_BASE: List[str] = [
    "Periodo",
    "Razón social",
    "CUIT",
    "Contrato",
    "Aseguradora",
    "Deuda total",
    "Costo mensual",
    "Q periodos deudores",
    "Estado contrato",
    "Email del trato",
    "No contactar",
    "Productor",
    "Premier",
    "Cliente importante",
]

# Columnas numéricas para formato
COLS_MONEDA = ["Deuda total", "Costo mensual"]
COL_Q = "Q periodos deudores"
COL_CUIT = "CUIT"

# =========================
# Helpers
# =========================

def _as_stripped(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def _is_empty_email(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return s.eq("") | s.str.lower().isin({"nan", "none"})

def _ensure_columns(df: pd.DataFrame, columnas: Iterable[str]) -> pd.DataFrame:
    """Garantiza la presencia de todas las columnas en `columnas`."""
    out = df.copy()
    for col in columnas:
        if col not in out.columns:
            out[col] = pd.NA
    # Reordenar
    out = out[columnas]
    return out

def _to_number(series: pd.Series, decimals: int | None = None) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    if decimals is not None:
        s = s.round(decimals)
    return s

def _normalize_cuit(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(r"[^0-9]", "", regex=True)
    # Exportación: queremos número entero; si no es convertible, dejar NaN y se limpia al escribir
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def _moneda_arg_pattern() -> str:
    # Formato Excel con miles '.' y decimales ',' (según tu requerimiento)
    return '"$ " #.##0,00'

def _q_pattern() -> str:
    return '0,00'

def _build_table_columns(df: pd.DataFrame, workbook, idx_cuit: int | None) -> List[Dict]:
    """Construye metadatos de columnas para xlsxwriter.add_table() con formatos."""
    fmt_pesos = workbook.add_format({"num_format": _moneda_arg_pattern(), "align": "right"})
    fmt_q = workbook.add_format({"num_format": _q_pattern(), "align": "right"})
    fmt_cuit = workbook.add_format({"num_format": "00000000000"})  # 11 dígitos fijos

    cols_fmt = []
    for i, name in enumerate(df.columns):
        if name in COLS_MONEDA:
            cols_fmt.append({"header": name, "format": fmt_pesos})
        elif name == COL_Q:
            cols_fmt.append({"header": name, "format": fmt_q})
        elif idx_cuit is not None and i == idx_cuit:
            cols_fmt.append({"header": name, "format": fmt_cuit})
        else:
            cols_fmt.append({"header": name})
    return cols_fmt


# =========================
# Derivación de hojas
# =========================

def derivar_hojas_consolidado(
    df_consolidado: pd.DataFrame,
    df_no_cruzan: Optional[pd.DataFrame] = None,
    capitas_lookup: Optional[Mapping[int | str, object]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Aplica la matriz de filtros sobre `df_consolidado` y devuelve
    { nombre_hoja: DataFrame } incluyendo 'Consolidado' y 'No cruzan'.

    - `capitas_lookup`: dict/Series mapeando CUIT -> Capitas, solo se usa en "Agregar costo mensual".
    """
    base = _ensure_columns(df_consolidado, COLUMNAS_BASE).copy()

    # Tipos esperados
    base["Productor"] = base["Productor"].fillna("")
    base["Productor"] = base["Productor"].replace("", "PROMECOR")
    base["Premier"] = _as_stripped(base["Premier"]).replace("", "No es Premier")

    # Numéricos
    base["Deuda total"] = _to_number(base["Deuda total"], 2)
    base["Costo mensual"] = _to_number(base["Costo mensual"], 2)
    base[COL_Q] = _to_number(base[COL_Q], 2)
    base[COL_CUIT] = _normalize_cuit(base[COL_CUIT])

    # Conveniencias
    vigente = _as_stripped(base["Estado contrato"]).str.casefold().eq("vigente")
    email_vacio = _is_empty_email(base["Email del trato"])
    premier_no = _as_stripped(base["Premier"]).str.casefold().eq("no es premier")
    premier_si = _as_stripped(base["Premier"]).str.casefold().eq("premier")
    no_contactar = base["No contactar"].astype(str).str.lower().isin(["true", "1", "verdadero", "sí", "si"])
    cliente_imp = base["Cliente importante"].astype(str).str.lower().isin(["true", "1", "verdadero", "sí", "si"])
    q = base[COL_Q]
    deuda = base["Deuda total"]

    hojas: Dict[str, pd.DataFrame] = {}

    # 1) Consolidado (tal cual)
    hojas["Consolidado"] = base.copy()

    # 2) No cruzan (mismas columnas). Si no viene, crear vacío.
    if df_no_cruzan is None:
        hojas["No cruzan"] = _ensure_columns(pd.DataFrame(columns=COLUMNAS_BASE), COLUMNAS_BASE)
    else:
        nc = _ensure_columns(df_no_cruzan.copy(), COLUMNAS_BASE)
        # Productor vacío => PROMECOR
        nc["Productor"] = nc["Productor"].replace("", "PROMECOR")
        hojas["No cruzan"] = nc

    # 3) Sin mail: Email vacío **y** Premier = "No es Premier"
    hojas["Sin mail"] = base[email_vacio & premier_no].copy()

    # 4) Anuladas: Estado != Vigente, quitando emails vacíos
    hojas["Anuladas"] = base[(~vigente) & (~email_vacio)].copy()

    # 5) No contactar: verdadero, Cliente imp = falso, Vigente, email no vacío
    hojas["No contactar"] = base[no_contactar & (~cliente_imp) & vigente & (~email_vacio)].copy()

    # 6) Clientes importantes: verdadero, No contactar = falso, Vigente, email no vacío
    hojas["Clientes importantes"] = base[cliente_imp & (~no_contactar) & vigente & (~email_vacio)].copy()

    # 7) 1 Q.deudor: Q ≤ 1 (con Q calculado), Vigente, email no vacío
    hojas["1 Q.deudor"] = base[q.notna() & (q <= 1) & vigente & (~email_vacio)].copy()

    # 8) Premier: Premier = "Premier", Vigente, email no vacío
    hojas["Premier"] = base[premier_si & vigente & (~email_vacio)].copy()

    # 9) Productor: ≠ PROMECOR, Q > 1, Vigente, email no vacío, Deuda ≥ 1000
    hojas["Productor"] = base[
        (base["Productor"].str.upper() != "PROMECOR")
        & q.notna() & (q > 1)
        & vigente
        & (~email_vacio)
        & deuda.ge(1000)
    ].copy()

    # 10) Deuda Promecor: = PROMECOR, Q > 1, Vigente, Cliente imp = falso, No contactar = falso,
    #     Premier = "No es Premier", email no vacío, Deuda ≥ 1000
    hojas["Deuda Promecor"] = base[
        (base["Productor"].str.upper() == "PROMECOR")
        & q.notna() & (q > 1)
        & vigente
        & (~cliente_imp)
        & (~no_contactar)
        & premier_no
        & (~email_vacio)
        & deuda.ge(1000)
    ].copy()

    # 11) Agregar costo mensual: Costo mensual vacío o 0 ⇒ Q vacío
    costo_vacio = base["Costo mensual"].isna() | base["Costo mensual"].eq(0)
    agregar = base[costo_vacio].copy()
    agregar[COL_Q] = pd.NA  # Q vacío
    # Agregar "Capitas" desde look-up (solo en esta hoja)
    if capitas_lookup is not None:
        # Normalizar claves del lookup a int o str comparables
        def _lkp(cuit_val):
            # cuit_val es Int64/NaN → pasarlo a int o str; probar int primero
            if pd.isna(cuit_val):
                return pd.NA
            key_int = int(cuit_val)
            if key_int in capitas_lookup:
                return capitas_lookup[key_int]
            key_str = str(key_int)
            return capitas_lookup.get(key_str, pd.NA)

        agregar["Capitas"] = agregar[COL_CUIT].map(_lkp)
    else:
        agregar["Capitas"] = pd.NA

    # Reordenar, colocando Capitas al final
    cols_agregar = COLUMNAS_BASE + ["Capitas"]
    hojas["Agregar costo mensual"] = _ensure_columns(agregar, cols_agregar)

    # Asegurar esquema idéntico en todas (salvo 'Agregar costo mensual' que tiene Capitas)
    for nombre, dfh in list(hojas.items()):
        if nombre != "Agregar costo mensual":
            hojas[nombre] = _ensure_columns(dfh, COLUMNAS_BASE)

    return hojas


# =========================
# Escritura a Excel
# =========================

def exportar_consolidado_completo(
    hojas: Dict[str, pd.DataFrame],
    out: str | Path | BytesIO,
    table_style: str = "Table Style Medium 2",
) -> None:
    """
    Escribe todas las hojas en un solo Excel, con tabla, formatos y pane freeze.
    `hojas` es el dict {nombre_hoja: df} retornado por `derivar_hojas_consolidado`.
    """
    out_is_path = isinstance(out, (str, Path))
    buffer = BytesIO() if out_is_path else out

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        wb = writer.book

        for nombre, df in hojas.items():
            df = df.copy()

            # Forzar tipos para formateo
            if COL_CUIT in df.columns:
                df[COL_CUIT] = _normalize_cuit(df[COL_CUIT])

            for c in COLS_MONEDA:
                if c in df.columns:
                    df[c] = _to_number(df[c], 2)

            if COL_Q in df.columns:
                df[COL_Q] = _to_number(df[COL_Q], 2)

            # Escribir
            df.to_excel(writer, index=False, sheet_name=nombre)
            ws = writer.sheets[nombre]

            # Índice de CUIT
            idx_cuit = list(df.columns).index(COL_CUIT) if COL_CUIT in df.columns else None

            # Tabla con formatos por columna
            cols_fmt = _build_table_columns(df, wb, idx_cuit)

            rows, cols = df.shape
            # add_table: (first_row, first_col, last_row, last_col)
            ws.add_table(0, 0, max(rows, 1), max(cols - 1, 0), {
                "name": f"tbl_{nombre.replace(' ', '_')[:25]}",
                "style": table_style,
                "columns": cols_fmt,
            })

            # Anchos sugeridos
            ancho = {
                "Periodo": 10, "Razón social": 35, "CUIT": 14, "Contrato": 14, "Aseguradora": 18,
                "Deuda total": 16, "Costo mensual": 16, "Q periodos deudores": 14, "Estado contrato": 18,
                "Email del trato": 34, "No contactar": 12, "Productor": 14, "Premier": 14,
                "Cliente importante": 16, "Capitas": 14,
            }
            for i, col in enumerate(df.columns):
                ws.set_column(i, i, ancho.get(col, 12))

            # Freeze encabezados
            ws.freeze_panes(1, 0)

    if out_is_path:
        # Guardar desde buffer
        assert isinstance(buffer, BytesIO)
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with open(out, "wb") as f:
            f.write(buffer.getvalue())


# ======================================================
# (Opcional) utilidades previas que ya tenías en el archivo
# ======================================================

def exportar_excel_tabla(
    df: pd.DataFrame,
    out: str | Path | BytesIO,
    *,
    sheet_name: str = "Consolidado",
    table_name: str = "tbl_consolidado",
) -> None:
    """
    Exporta un único DataFrame con formato de tabla y formatos de columnas.
    Se mantiene por compatibilidad (cuando quieras escribir solo una hoja).
    """
    out_is_path = isinstance(out, (str, Path))
    buffer = BytesIO() if out_is_path else out

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df = _ensure_columns(df, COLUMNAS_BASE).copy()
        df[COL_CUIT] = _normalize_cuit(df[COL_CUIT])
        for c in COLS_MONEDA:
            df[c] = _to_number(df[c], 2)
        df[COL_Q] = _to_number(df[COL_Q], 2)

        df.to_excel(writer, index=False, sheet_name=sheet_name)
        wb, ws = writer.book, writer.sheets[sheet_name]

        idx_cuit = list(df.columns).index(COL_CUIT)
        cols_fmt = _build_table_columns(df, wb, idx_cuit)

        rows, cols = df.shape
        ws.add_table(
            0, 0, max(rows, 1), max(cols - 1, 0),
            {"name": table_name[:31], "style": "Table Style Medium 2", "columns": cols_fmt},
        )

        # Anchos y freeze
        ws.set_column(0, cols - 1, 12)
        ws.set_column(idx_cuit, idx_cuit, 14)
        ws.set_column(df.columns.get_loc("Razón social"), df.columns.get_loc("Razón social"), 35)
        ws.set_column(df.columns.get_loc("Email del trato"), df.columns.get_loc("Email del trato"), 34)
        ws.freeze_panes(1, 0)

    if out_is_path:
        assert isinstance(buffer, BytesIO)
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        with open(out, "wb") as f:
            f.write(buffer.getvalue())


# =========================
# Back-compat para imports
# =========================

def leer_maestro(path):
    """
    Compatibilidad: varios módulos importan esto desde art.utils.consolidado.
    Acá solo leemos el Excel tal cual (dtype=str). El mapeo lo hace quien lo use.
    """
    return pd.read_excel(path, sheet_name=0, dtype=str)

def leer_aseguradora(path, aseguradora=None):
    """
    Compatibilidad: lectura simple del Excel de aseguradora.
    """
    return pd.read_excel(path, sheet_name=0)

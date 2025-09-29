# art/services/persistencia_consolidado.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, List, Iterable
import hashlib
import json

import pandas as pd

from art.models import ConsolidadoLote, ConsolidadoItem


# --------------------------
# Helpers
# --------------------------
def _parse_periodo(periodo_str: str) -> date:
    """
    Convierte 'MM-AAAA' o 'M-AAAA' (también 'MM/AAAA' o 'AAAA-MM') en date(AAAA, MM, 1).
    """
    if not periodo_str:
        raise ValueError("periodo_str vacío")
    s = periodo_str.strip().replace("/", "-")
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"Formato de periodo no soportado: {periodo_str}")
    a, b = parts
    if len(a) == 4:
        yyyy, mm = int(a), int(b)
    else:
        mm, yyyy = int(a), int(b)
    return date(year=yyyy, month=mm, day=1)


def _to_decimal(val) -> Decimal:
    """
    Convierte valores como '$ 100.000,00', '100.000,00', 100000.0 a Decimal.
    NaN/None -> Decimal('0').
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return Decimal("0")
    if isinstance(val, (int, float, Decimal)):
        try:
            return Decimal(str(val))
        except InvalidOperation:
            return Decimal("0")
    s = str(val).strip()
    if not s:
        return Decimal("0")
    s = (
        s.replace("U$S", "")
         .replace("u$s", "")
         .replace("$", "")
         .replace(".", "")
         .replace(" ", "")
         .replace(",", ".")
    )
    try:
        return Decimal(s)
    except InvalidOperation:
        return Decimal("0")


# --- FLAGS: versión estricta ---
_TRUE_TOKENS = {"verdadero", "true", "si", "sí", "1"}

def _to_bool_flag_strict(val) -> bool:
    """
    Solo devuelve True si el valor es claramente verdadero:
    'verdadero', 'true', 'si', 'sí', '1' o boolean True.
    Cualquier otro texto (incluido 'No es Premier', 'ok', '-') -> False.
    """
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    s = str(val).strip().lower()
    if s in _TRUE_TOKENS:
        return True
    # si es numérico ≠ 0 lo consideramos True
    try:
        return float(s.replace(",", ".")) != 0.0
    except Exception:
        return False


def _get_from_low(low: Dict[str, any], keys: Iterable[str], default="") -> str:
    for k in keys:
        if k in low and str(low[k]).strip():
            return str(low[k]).strip()
    return default


# --------------------------
# Mapeo por fila (DataFrame -> ConsolidadoItem)
# --------------------------
def _row_to_item_kwargs(row: dict, periodo: date, hoja: str) -> dict:
    """
    Mapea una fila del DF a kwargs de ConsolidadoItem.
    Tolerante a variaciones de encabezados (case-insensitive + sinónimos).
    """
    low = { (k or "").strip().lower(): v for k, v in row.items() }

    # Identificadores y contexto
    razon_social = _get_from_low(low, [
        "razón social", "razon social", "razon_social", "razón social (nombre de cuenta)"
    ])
    cuit = _get_from_low(low, ["cuit"])
    aseguradora = _get_from_low(low, ["aseguradora"])
    contrato = _get_from_low(low, [
        "nro. contrato", "nro contrato", "nro de contrato", "nro. de contrato",
        "número de contrato", "numero de contrato",
        "nº de contrato", "n° de contrato", "nº contrato", "n° contrato",
        "contrato"
    ])
    estado_contrato = _get_from_low(low, ["estado contrato", "estado", "estado_contrato"])
    productor = _get_from_low(low, ["productor"], default="PROMECOR")
    email_trato = _get_from_low(low, ["email del trato", "email_del_trato", "email"])

    # Métricas
    q_per_raw = low.get("q periodos deudores", low.get("q períodos deudores", low.get("q_periodos_deudores")))
    try:
        q_per = None if q_per_raw in (None, "") or (isinstance(q_per_raw, float) and pd.isna(q_per_raw)) else Decimal(str(q_per_raw))
    except InvalidOperation:
        q_per = None

    deuda = _to_decimal(low.get("deuda_total", low.get("deuda total", low.get("deuda", 0))))
    costo_mensual = low.get("costo_mensual", low.get("costo mensual"))
    costo_mensual = None if costo_mensual is None else _to_decimal(costo_mensual)

    # Flags (incluye variantes '(... Nombre de Cuenta)')
    no_contactar = _to_bool_flag_strict(
        low.get("no contactar", low.get("no_contactar", low.get("no contactar (nombre de cuenta)", False)))
    )

    # PREMIER: ESTRICTO → solo "Premier" (case-insensitive) produce "Premier"
    premier_raw = (low.get("premier") or low.get("premier (nombre de cuenta)") or "")
    premier = "Premier" if str(premier_raw).strip().lower() == "premier" else "No es Premier"

    cliente_importante = _to_bool_flag_strict(
        low.get("cliente importante",
        low.get("cliente_importante",
        low.get("cliente importante (nombre de cuenta)", False)))
    )

    kwargs = dict(
        cuit=cuit,
        periodo=periodo,
        razon_social=razon_social,
        aseguradora=aseguradora,
        contrato=str(contrato) if contrato else "",
        deuda_total=deuda,
        costo_mensual=costo_mensual,
        q_periodos_deudores=q_per,
        estado_contrato=estado_contrato,
        email_del_trato=email_trato,
        no_contactar=no_contactar,
        productor=productor or "PROMECOR",
        premier=premier,
        cliente_importante=cliente_importante,
        en_deuda=(deuda > 0),
        hoja=hoja,
    )

    # Extras (excluimos claves reconocidas)
    recognized = {
        "razón social", "razon social", "razon_social", "razón social (nombre de cuenta)",
        "cuit", "aseguradora",
        "nro. contrato", "nro contrato", "nro de contrato", "nro. de contrato",
        "número de contrato", "numero de contrato", "nº de contrato", "n° de contrato",
        "nº contrato", "n° contrato", "contrato",
        "estado contrato", "estado", "estado_contrato", "productor",
        "email del trato", "email_del_trato", "email",
        "q periodos deudores", "q períodos deudores", "q_periodos_deudores",
        "deuda_total", "deuda total", "deuda",
        "costo_mensual", "costo mensual",
        "no contactar", "no_contactar", "no contactar (nombre de cuenta)",
        "premier", "premier (nombre de cuenta)",
        "cliente importante", "cliente_importante", "cliente importante (nombre de cuenta)",
    }
    kwargs["extra"] = {k: v for k, v in row.items() if (k or "").strip().lower() not in recognized}
    return kwargs


def _calc_hash(periodo_str: str,
               dfC: Optional[pd.DataFrame],
               dfN: Optional[pd.DataFrame],
               dfP: Optional[pd.DataFrame],
               archivos_fuente: Optional[Dict[str, str]]) -> str:
    """
    Hash razonable de la entrada para detectar duplicados exactos (opcional).
    """
    h = hashlib.sha256()
    h.update(periodo_str.encode("utf-8"))
    h.update(json.dumps(archivos_fuente or {}, sort_keys=True, ensure_ascii=False).encode("utf-8"))
    for tag, df in (("C", dfC), ("N", dfN), ("P", dfP)):
        if df is None or df.empty:
            h.update(f"{tag}-empty".encode())
            continue
        h.update(tag.encode())
        cols = [str(c) for c in df.columns]
        h.update(",".join(cols).lower().encode("utf-8"))
        h.update(str(len(df)).encode("utf-8"))
        try:
            sample = df.head(200).to_csv(index=False)
        except Exception:
            sample = json.dumps(df.head(200).to_dict(orient="records"), ensure_ascii=False)
        h.update(sample.encode("utf-8", errors="ignore"))
    return h.hexdigest()


# --------------------------
# API principal
# --------------------------
@dataclass
class GuardadoResultado:
    lote: ConsolidadoLote
    items_creados: int
    duplicado: bool = False


def guardar_lote_y_items(
    *,
    usuario,
    periodo_str: str,
    df_consolidado: Optional[pd.DataFrame],
    df_no_cruzan: Optional[pd.DataFrame],
    df_productor: Optional[pd.DataFrame],
    nombre_archivo_maestro: str = "",
    archivos_fuente: Optional[Dict[str, str]] = None,
    ruta_excel_salida: str = "",
    observaciones: str = "",
    calcular_hash: bool = True,
    evitar_duplicado_por_hash: bool = False,
    reemplazar_periodo: bool = False,
) -> GuardadoResultado:
    """
    Crea un ConsolidadoLote + ConsolidadoItem en bulk a partir de los DataFrames.

    - calcular_hash: si True, guarda hash_entrada en el lote.
    - evitar_duplicado_por_hash: si True y existe un lote con igual hash_entrada, no inserta nada y devuelve duplicado=True.
    - reemplazar_periodo: si True, borra items de ese periodo antes de insertar (todas las hojas/lotes previos del mismo periodo).
    """
    archivos_fuente = archivos_fuente or {}
    periodo = _parse_periodo(periodo_str)

    def norm_df(df: Optional[pd.DataFrame]) -> pd.DataFrame:
        return pd.DataFrame() if df is None else df

    dfC = norm_df(df_consolidado)
    dfN = norm_df(df_no_cruzan)
    dfP = norm_df(df_productor)

    entrada_hash = ""
    if calcular_hash:
        entrada_hash = _calc_hash(periodo_str, dfC, dfN, dfP, archivos_fuente)
        if evitar_duplicado_por_hash:
            if ConsolidadoLote.objects.filter(hash_entrada=entrada_hash).exists():
                lote_existente = ConsolidadoLote.objects.filter(hash_entrada=entrada_hash).order_by("-id").first()
                return GuardadoResultado(lote=lote_existente, items_creados=0, duplicado=True)

    if reemplazar_periodo:
        ConsolidadoItem.objects.filter(periodo=periodo).delete()

    lote = ConsolidadoLote.objects.create(
        usuario=usuario,
        nombre_archivo_maestro=nombre_archivo_maestro or "",
        archivos_fuente=archivos_fuente,
        ruta_excel_salida=ruta_excel_salida or "",
        filas_consolidado=len(dfC),
        filas_no_cruzan=len(dfN),
        observaciones=observaciones or "",
        hash_entrada=entrada_hash,
    )

    items: List[ConsolidadoItem] = []

    def extend_items(df: pd.DataFrame, hoja: str):
        if df.empty:
            return
        for row in df.to_dict(orient="records"):
            kwargs = _row_to_item_kwargs(row, periodo=periodo, hoja=hoja)
            items.append(ConsolidadoItem(lote=lote, **kwargs))

    extend_items(dfC, "consolidado")
    extend_items(dfN, "no_cruzan")
    extend_items(dfP, "productor")

    if items:
        ConsolidadoItem.objects.bulk_create(items, batch_size=2000)

    return GuardadoResultado(lote=lote, items_creados=len(items), duplicado=False)

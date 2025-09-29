# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

import pandas as pd

from art.models import ArtDashboardContratoPeriodo

# ===== Helpers de parsing =====

DEC2 = Decimal("0.01")
DEC4 = Decimal("0.0001")

REQUIRED_COLS = [
    "Periodo", "CUIT", "Contrato", "Aseguradora", "Deuda total",
    "Q periodos deudores",
]

TRUE_WORDS = {"si", "sí", "true", "verdadero", "1", "x", "yes", "y"}
FALSE_WORDS = {"no", "false", "falso", "0", ""}

def normalize_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def only_digits(x: str) -> str:
    return re.sub(r"\D+", "", x or "")

def parse_ars(value) -> Decimal | None:
    """
    Convierte '$ 1.234.567,89' -> Decimal('1234567.89')
    Soporta valores vacíos o NaN.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None
    s = s.replace("$", "").replace("ARS", "").replace("U$S", "")
    s = re.sub(r"[^\d,.\-]", "", s)
    # coma como decimal (formato AR)
    if "," in s and s.count(",") == 1 and (s.rfind(",") > s.rfind(".")):
        s = s.replace(".", "")
        s = s.replace(",", ".")
    try:
        return Decimal(s).quantize(DEC2, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None

def parse_decimal(value, quant=DEC4) -> Decimal | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip().replace(",", ".")
    if s == "":
        return None
    try:
        d = Decimal(s)
        return d.quantize(quant, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None

def parse_bool_generic(value) -> bool:
    if pd.isna(value):
        return False
    s = str(value).strip().lower()
    if "no es premier" in s:
        return False
    if s == "premier":
        return True
    if s in TRUE_WORDS:
        return True
    if s in FALSE_WORDS:
        return False
    return bool(s)

def parse_periodo_str(p: str) -> date:
    """
    '06-2025' -> date(2025,6,1)
    También soporta '2025-06' o '2025/06'.
    """
    s = (p or "").strip()
    if re.fullmatch(r"\d{2}-\d{4}", s):
        m, y = s.split("-")
        return date(int(y), int(m), 1)
    if re.fullmatch(r"\d{4}[-/]\d{2}", s):
        y, m = re.split(r"[-/]", s)
        return date(int(y), int(m), 1)
    raise ValueError(f"Formato de período no reconocido: '{p}' (esperado 'MM-YYYY')")

def bucket_q(value: Decimal | None) -> str:
    if value is None:
        return ""
    try:
        v = float(value)
    except Exception:
        return ""
    if v < 1.5:
        return "1"
    if v < 2.5:
        return "2"
    if v < 3.5:
        return "3"
    if v < 6:
        return "4-5"
    return "6+"

# ===== Lector de hoja =====

def read_consolidado_sheet(archivo: str, hoja: str | None):
    """
    Lee una hoja específica o autodetecta una hoja que contenga las columnas requeridas.
    Retorna un DataFrame de strings.
    """
    if hoja and hoja.lower() != "auto":
        df = pd.read_excel(archivo, sheet_name=hoja, dtype=str)
        for c in df.columns:
            df[c] = df[c].apply(normalize_str)
        return df, hoja

    # autodetección
    all_sheets = pd.read_excel(archivo, sheet_name=None, dtype=str)
    for nombre, df in all_sheets.items():
        cols = list(df.columns)
        if all(col in cols for col in REQUIRED_COLS):
            for c in df.columns:
                df[c] = df[c].apply(normalize_str)
            return df, nombre
    raise CommandError(
        f"No se encontró ninguna hoja con todas las columnas requeridas: {REQUIRED_COLS}"
    )

# ===== Command =====

class Command(BaseCommand):
    help = "Importa/actualiza la tabla de análisis ART desde el XLSX consolidado."

    def add_arguments(self, parser):
        parser.add_argument(
            "--archivo",
            required=True,
            help="Ruta del XLSX consolidado (ej: C:\\Users\\Promecor\\Documents\\...\\Consolidado_ART_06-2025.xlsx)",
        )
        parser.add_argument(
            "--hoja",
            default="Promecor",
            help="Nombre de la hoja (por defecto 'Promecor'). Usá 'auto' para autodetectar.",
        )
        parser.add_argument(
            "--periodo",
            default=None,
            help="Periodo MM-YYYY (si se omite, se toma de la columna 'Periodo' del archivo).",
        )
        parser.add_argument(
            "--lote",
            default="",
            help="Identificador libre para trazabilidad (ej: '2025-06').",
        )

    def handle(self, *args, **options):
        archivo = options["archivo"]
        hoja = options["hoja"]
        periodo_cli = options["periodo"]
        lote_ref = options["lote"]

        try:
            df, hoja_usada = read_consolidado_sheet(archivo, hoja)
        except Exception as e:
            raise CommandError(f"No se pudo leer el archivo/hoja: {e}")

        # Validación de columnas
        cols = list(df.columns)
        faltantes = [c for c in REQUIRED_COLS if c not in cols]
        if faltantes:
            raise CommandError(
                f"Faltan columnas obligatorias en '{hoja_usada}': {faltantes}. "
                f"Columnas disponibles: {cols}"
            )

        # Determinar período (global o por fila)
        periodo_global = None
        if periodo_cli:
            try:
                periodo_global = parse_periodo_str(periodo_cli)
            except Exception as e:
                raise CommandError(str(e))

        creados = 0
        actualizados = 0
        errores = 0

        with transaction.atomic():
            for i, row in df.iterrows():
                try:
                    periodo = periodo_global or parse_periodo_str(row.get("Periodo", ""))
                    razon_social = row.get("Razón social", "")
                    cuit = only_digits(row.get("CUIT", ""))[:20]
                    contrato = row.get("Contrato", "")
                    aseguradora = row.get("Aseguradora", "")

                    deuda_total = parse_ars(row.get("Deuda total"))
                    costo_mensual = parse_ars(row.get("Costo mensual"))
                    qpd = parse_decimal(row.get("Q periodos deudores"), quant=DEC4)

                    estado_contrato = row.get("Estado contrato", "")
                    email = row.get("Email del trato", "")
                    no_contactar = parse_bool_generic(row.get("No contactar"))
                    productor = row.get("Productor", "")
                    premier = parse_bool_generic(row.get("Premier"))
                    cliente_imp = parse_bool_generic(row.get("Cliente importante"))

                    riesgo = False
                    if qpd is not None:
                        try:
                            riesgo = (qpd >= Decimal("2"))
                        except Exception:
                            riesgo = False

                    bq = bucket_q(qpd)

                    deuda_vs_costo = None
                    if deuda_total is not None and costo_mensual:
                        try:
                            if costo_mensual > 0:
                                deuda_vs_costo = (deuda_total / costo_mensual).quantize(DEC4, rounding=ROUND_HALF_UP)
                        except Exception:
                            deuda_vs_costo = None

                    # Upsert por clave única
                    obj, created = ArtDashboardContratoPeriodo.objects.update_or_create(
                        periodo=periodo,
                        cuit=cuit,
                        contrato=contrato,
                        aseguradora=aseguradora,
                        defaults=dict(
                            razon_social=razon_social,
                            deuda_total=deuda_total or Decimal("0.00"),
                            costo_mensual=costo_mensual,
                            q_periodos_deudores=qpd,
                            estado_contrato=estado_contrato,
                            email_trato=email,
                            no_contactar=no_contactar,
                            productor=productor,
                            premier=premier,
                            cliente_importante=cliente_imp,
                            riesgo_flag=riesgo,
                            bucket_q=bq,
                            deuda_vs_costo=deuda_vs_costo,
                            lote_ref=lote_ref or "",
                        ),
                    )
                    if created:
                        creados += 1
                    else:
                        actualizados += 1

                except Exception as e:
                    errores += 1
                    raise CommandError(f"Error en fila {i+2} (contando encabezado): {e}")

        self.stdout.write(self.style.SUCCESS(
            f"Importación finalizada (hoja: {hoja_usada}). Creados: {creados} | Actualizados: {actualizados} | Errores: {errores}"
        ))

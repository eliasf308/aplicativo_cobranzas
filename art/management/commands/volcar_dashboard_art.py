# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from art.models import (
    ArtDashboardContratoPeriodo,
    ConsolidadoItem,
    ConsolidadoLote,
)

# ------------- Helpers genéricos -------------
DEC2 = Decimal("0.01")
DEC4 = Decimal("0.0001")

TRUE_WORDS = {"si", "sí", "true", "verdadero", "1", "x", "yes", "y"}
FALSE_WORDS = {"no", "false", "falso", "0", ""}

def to_str(x) -> str:
    return "" if x is None else str(x).strip()

def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")

def to_decimal(x, quant=DEC2) -> Decimal | None:
    if x is None or to_str(x) == "":
        return None
    if isinstance(x, (int, float, Decimal)):
        try:
            return (Decimal(str(x))).quantize(quant, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None
    s = to_str(x)
    # Soporta "$ 1.234.567,89" / "1.234.567,89" / "1234567.89"
    s = s.replace("$", "").replace("ARS", "").replace("U$S", "")
    s = re.sub(r"[^\d,.\-]", "", s)
    if "," in s and s.count(",") == 1 and (s.rfind(",") > s.rfind(".")):
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s).quantize(quant, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None

def to_bool_generic(x) -> bool:
    s = to_str(x).lower()
    if "no es premier" in s:
        return False
    if s == "premier":
        return True
    if s in TRUE_WORDS:
        return True
    if s in FALSE_WORDS:
        return False
    return bool(s)

def parse_periodo_any(p) -> date:
    """
    Acepta:
      - datetime.date -> normaliza a primer día del mes
      - 'MM-YYYY'
      - 'YYYY-MM' o 'YYYY/MM'
      - 'YYYY-MM-DD' -> toma YYYY-MM y arma primer día
    """
    if isinstance(p, date):
        return date(p.year, p.month, 1)
    s = to_str(p)
    if re.fullmatch(r"\d{2}-\d{4}", s):  # MM-YYYY
        mm, yyyy = s.split("-")
        return date(int(yyyy), int(mm), 1)
    if re.fullmatch(r"\d{4}[-/]\d{2}", s):  # YYYY-MM o YYYY/MM
        yyyy, mm = re.split(r"[-/]", s)
        return date(int(yyyy), int(mm), 1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):  # YYYY-MM-DD
        yyyy, mm, _dd = s.split("-")
        return date(int(yyyy), int(mm), 1)
    raise ValueError(f"Formato de período no reconocido: {repr(p)} (usa 'MM-YYYY' o 'YYYY-MM').")

def bucket_q(val: Decimal | None) -> str:
    if val is None:
        return ""
    try:
        v = float(val)
    except Exception:
        return ""
    if v < 1.5: return "1"
    if v < 2.5: return "2"
    if v < 3.5: return "3"
    if v < 6:   return "4-5"
    return "6+"

def get_attr(obj, names: list[str], default=None):
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return default

# ------------- Core de volcado -------------
def iter_items_filtrados(lote_id: int | None, periodo: date | None):
    qs = ConsolidadoItem.objects.all()

    # Filtro por lote (intentamos ambas formas)
    if lote_id is not None:
        try:
            qs = qs.filter(lote_id=lote_id)
        except Exception:
            try:
                qs = qs.filter(lote__id=lote_id)
            except Exception:
                pass

    # Si no hay filtro por período, devolvemos directo
    if periodo is None:
        yield from (qs.iterator() if hasattr(qs, "iterator") else qs)
        return

    # Con filtro por período, normalizamos comparando mes/año
    for it in (qs.iterator() if hasattr(qs, "iterator") else qs):
        p_val = get_attr(it, ["periodo", "periodo_str", "mes_periodo", "period"])
        if not p_val:
            lote_obj = get_attr(it, ["lote"])
            if lote_obj:
                p_val = get_attr(lote_obj, ["periodo", "periodo_str", "mes_periodo", "period"])
        try:
            p_date = parse_periodo_any(p_val)
            if p_date == periodo:
                yield it
        except Exception:
            continue

def build_dashboard_row_from_item(it, periodo: date | None, lote_id: int | None):
    razon_social = to_str(get_attr(it, ["razon_social", "razon", "razon_social_cliente"]))
    cuit         = only_digits(to_str(get_attr(it, ["cuit", "CUIT"])))[:20]
    contrato     = to_str(get_attr(it, ["contrato", "numero_contrato", "nro_contrato", "nro_contrato_art"]))
    aseguradora  = to_str(get_attr(it, ["aseguradora", "compania", "compania_art", "aseguradora_nombre"]))

    deuda_total      = to_decimal(get_attr(it, ["deuda_total", "deuda", "saldo_deuda", "saldo_total", "importe_deuda"]), quant=DEC2) or Decimal("0.00")
    costo_mensual    = to_decimal(get_attr(it, ["costo_mensual", "costo", "costo_mensual_estimado", "costo_promedio"]), quant=DEC2)
    q_deudores       = to_decimal(get_attr(it, ["q_periodos_deudores", "q_deudores", "q_periodos", "meses_deuda"]), quant=DEC4)

    estado_contrato  = to_str(get_attr(it, ["estado_contrato", "estado"]))
    email_trato      = to_str(get_attr(it, ["email_del_trato", "email_trato", "email", "correo"]))
    no_contactar     = to_bool_generic(get_attr(it, ["no_contactar", "excluir_envio", "excluir"]))
    productor        = to_str(get_attr(it, ["productor", "productor_nombre", "ejecutivo"]))
    premier          = to_bool_generic(get_attr(it, ["premier", "es_premier", "flag_premier"]))
    cliente_imp      = to_bool_generic(get_attr(it, ["cliente_importante", "flag_cliente_importante", "cliente_important"]))

    per_val = periodo
    if per_val is None:
        p_item = get_attr(it, ["periodo", "periodo_str", "mes_periodo", "period"])
        if p_item:
            per_val = parse_periodo_any(p_item)
        else:
            lote_obj = get_attr(it, ["lote"])
            if lote_obj:
                p_lote = get_attr(lote_obj, ["periodo", "periodo_str", "mes_periodo", "period"])
                if p_lote:
                    per_val = parse_periodo_any(p_lote)
    if per_val is None:
        raise ValueError("No se pudo determinar el período para el item.")

    riesgo = False
    if q_deudores is not None:
        try:
            riesgo = (q_deudores >= Decimal("2"))
        except Exception:
            riesgo = False

    bq = bucket_q(q_deudores)

    deuda_vs_costo = None
    if deuda_total is not None and costo_mensual not in (None, Decimal("0"), Decimal("0.00")):
        try:
            if costo_mensual > 0:
                deuda_vs_costo = (deuda_total / costo_mensual).quantize(DEC4, rounding=ROUND_HALF_UP)
        except Exception:
            deuda_vs_costo = None

    lote_ref = to_str(lote_id) if lote_id is not None else ""
    return dict(
        periodo=per_val,
        razon_social=razon_social,
        cuit=cuit,
        contrato=contrato,
        aseguradora=aseguradora,
        deuda_total=deuda_total,
        costo_mensual=costo_mensual,
        q_periodos_deudores=q_deudores,
        estado_contrato=estado_contrato,
        email_trato=email_trato,
        no_contactar=no_contactar,
        productor=productor,
        premier=premier,
        cliente_importante=cliente_imp,
        riesgo_flag=riesgo,
        bucket_q=bq,
        deuda_vs_costo=deuda_vs_costo,
        lote_ref=lote_ref,
    )

# ------------- Management Command -------------
class Command(BaseCommand):
    help = "Vuelca ConsolidadoItem/ConsolidadoLote a ArtDashboardContratoPeriodo (panel ART), sin pasar por XLSX."

    def add_arguments(self, parser):
        parser.add_argument("--desde-lote", type=int, default=None,
                            help="ID de ConsolidadoLote para volcar (opcional).")
        parser.add_argument("--periodo", default=None,
                            help="Período MM-YYYY para filtrar items (opcional).")
        parser.add_argument("--reset-periodo", action="store_true",
                            help="Si se indica, borra previamente las filas del período destino antes de volcar.")
        parser.add_argument("--aseguradora", default=None,
                            help="Filtrar por aseguradora (opcional).")
        parser.add_argument("--productor", default=None,
                            help="Filtrar por productor (opcional).")

    def handle(self, *args, **options):
        lote_id = options.get("desde-lote")
        periodo_cli = options.get("periodo")
        reset_periodo = bool(options.get("reset-periodo"))
        filtro_aseg = to_str(options.get("aseguradora")) or None
        filtro_prod = to_str(options.get("productor")) or None

        periodo_date = None
        if periodo_cli:
            try:
                periodo_date = parse_periodo_any(periodo_cli)
            except Exception as e:
                raise CommandError(str(e))

        if lote_id is None and periodo_date is None:
            self.stdout.write(self.style.WARNING(
                "No se indicó --desde-lote ni --periodo. Se volcarán TODOS los items (puede tardar)."
            ))

        if reset_periodo and periodo_date is None:
            raise CommandError("--reset-periodo requiere indicar --periodo MM-YYYY.")

        items = list(iter_items_filtrados(lote_id, periodo_date))

        if filtro_aseg:
            items = [it for it in items if to_str(get_attr(it, ["aseguradora", "compania", "compania_art", "aseguradora_nombre"])).lower() == filtro_aseg.lower()]
        if filtro_prod:
            items = [it for it in items if to_str(get_attr(it, ["productor", "productor_nombre", "ejecutivo"])).lower() == filtro_prod.lower()]

        if not items:
            self.stdout.write(self.style.WARNING("No se encontraron items para volcar con los filtros indicados."))
            return

        if reset_periodo:
            borradas, _ = ArtDashboardContratoPeriodo.objects.filter(periodo=periodo_date).delete()
            self.stdout.write(self.style.WARNING(f"Reset periodo {periodo_date.strftime('%m-%Y')}: {borradas} filas eliminadas."))

        creados = 0
        actualizados = 0
        errores = 0

        with transaction.atomic():
            for it in items:
                try:
                    data = build_dashboard_row_from_item(it, periodo_date, lote_id)
                    if not (data["cuit"] and data["contrato"] and data["aseguradora"]):
                        continue

                    obj, created = ArtDashboardContratoPeriodo.objects.update_or_create(
                        periodo=data["periodo"],
                        cuit=data["cuit"],
                        contrato=data["contrato"],
                        aseguradora=data["aseguradora"],
                        defaults=data,
                    )
                    if created: creados += 1
                    else: actualizados += 1
                except Exception as e:
                    errores += 1
                    raise CommandError(f"Error al volcar item ID={getattr(it,'id', '?')}: {e}")

        self.stdout.write(self.style.SUCCESS(
            f"Volcado finalizado. Items procesados: {len(items)} | Creados: {creados} | Actualizados: {actualizados} | Errores: {errores}"
        ))

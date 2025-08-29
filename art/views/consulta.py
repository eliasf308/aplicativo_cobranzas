# art/views/consulta.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from decimal import Decimal
import json
from typing import List, Dict, Iterable

from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, Http404
from django.shortcuts import render, redirect
from django.urls import reverse

from art.models import ConsolidadoItem, EnvioEmailLog, ConsolidadoArt


@login_required
def consulta_busqueda_view(request: HttpRequest):
    if request.method == "POST":
        cuit = (request.POST.get("cuit") or "").strip()
        if cuit:
            return redirect(reverse("art:consulta_detalle", args=[cuit]))
    return render(request, "art_app/art/consulta.html")


def _format_ars(value: Decimal | float | int | str) -> str:
    try:
        q = Decimal(str(value))
    except Exception:
        q = Decimal("0")
    s = f"{q:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"$ {s}"


# ---------- helpers para fallback desde extra ----------
_CONTRATO_KEYS = {
    "nro. contrato", "nro contrato", "nro de contrato", "nro. de contrato",
    "número de contrato", "numero de contrato", "nº de contrato", "n° de contrato",
    "nº contrato", "n° contrato", "contrato"
}
_RAZON_KEYS = {"razón social", "razon social", "razon_social", "razón social (nombre de cuenta)"}
_ASEGURADORA_KEYS = {"aseguradora"}
_EMAIL_TRATO_KEYS = {"email del trato", "email_del_trato", "email"}
_QPER_KEYS = {"q periodos deudores", "q períodos deudores", "q_periodos_deudores"}
_ESTADO_KEYS = {"estado contrato", "estado", "estado_contrato"}
_PRODUCTOR_KEYS = {"productor"}
_PREMIER_KEYS = {"premier", "premier (nombre de cuenta)"}
_NO_CONTACTAR_KEYS = {"no contactar", "no_contactar", "no contactar (nombre de cuenta)"}
_CLIENTE_IMPORTANTE_KEYS = {"cliente importante", "cliente_importante", "cliente importante (nombre de cuenta)"}
_COSTO_KEYS = {"costo mensual", "costo_mensual"}


def _get_from_extra(extra: dict, keys: Iterable[str]) -> str:
    if not isinstance(extra, dict):
        return ""
    low = {(k or "").strip().lower(): v for k, v in extra.items()}
    for k in keys:
        if k in low and (low[k] is not None) and str(low[k]).strip():
            return str(low[k]).strip()
    return ""


def _has_any_key(extra: dict, keys: Iterable[str]) -> bool:
    if not isinstance(extra, dict):
        return False
    low = {(k or "").strip().lower(): v for k, v in extra.items()}
    return any(k in low for k in keys)


# ---------- lógica estricta de flags ----------
_TRUE_TOKENS = {"verdadero", "true", "si", "sí", "1"}

def _is_true_strict(v: object) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in _TRUE_TOKENS


@login_required
def consulta_detalle_view(request: HttpRequest, cuit: str):
    cuit = (cuit or "").strip()
    if not cuit:
        raise Http404("CUIT no provisto")

    # Solo hoja "consolidado"
    qs_all = (ConsolidadoItem.objects
              .filter(cuit=cuit, hoja="consolidado")
              .order_by("-periodo", "-deuda_total"))

    if not qs_all.exists():
        return render(request, "art_app/art/consulta_detalle.html", {
            "cuit": cuit,
            "sin_datos": True,
        })

    # ---------- Tomamos el ÚLTIMO PERÍODO disponible para badges ----------
    ultimo_periodo = qs_all.first().periodo  # gracias al order_by("-periodo")
    qs = qs_all.filter(periodo=ultimo_periodo)

    # ---------- TÍTULO (Razón Social) ----------
    razon = next((it.razon_social for it in qs if (it.razon_social or "").strip()), "")
    if not razon:
        for it in qs:
            val = _get_from_extra(it.extra, _RAZON_KEYS)
            if val:
                razon = val
                break
    # Fallback opcional a ConsolidadoArt solo para el título
    if not razon:
        art_row = ConsolidadoArt.objects.filter(cuit=cuit).order_by("-created_at").first()
        if art_row and (art_row.razon_social or "").strip():
            razon = art_row.razon_social
    titulo = razon or f"CUIT {cuit}"

    # ---------- CONTRATO / ASEGURADORA (agregados de TODO el histórico) ----------
    contratos_set, aseguradoras_set = set(), set()
    for it in qs_all:
        if (it.contrato or "").strip():
            contratos_set.add(it.contrato.strip())
        if (it.aseguradora or "").strip():
            aseguradoras_set.add(it.aseguradora.strip())
        v = _get_from_extra(it.extra, _CONTRATO_KEYS)
        if v:
            contratos_set.add(v)
        v = _get_from_extra(it.extra, _ASEGURADORA_KEYS)
        if v:
            aseguradoras_set.add(v)

    contrato_display = ""
    if len(contratos_set) == 1:
        contrato_display = sorted(contratos_set)[0]
    elif len(contratos_set) > 1:
        contrato_display = f"Varios ({len(contratos_set)})"

    if len(aseguradoras_set) == 1:
        aseguradora_display = sorted(aseguradoras_set)[0]
    elif len(aseguradoras_set) > 1:
        aseguradora_display = f"Varias ({len(aseguradoras_set)})"
    else:
        aseguradora_display = ""

    # ---------- Otros campos (último no vacío en ÚLTIMO PERÍODO; fallback extra) ----------
    def last_nonempty(attr: str, fallback_keys: set[str] | None = None, default=""):
        for it in qs:
            val = getattr(it, attr, "")
            if val is not None and str(val).strip():
                return val
            if fallback_keys:
                v2 = _get_from_extra(it.extra, fallback_keys)
                if v2:
                    return v2
        return default

    q_periodos = last_nonempty("q_periodos_deudores", _QPER_KEYS, default=None)
    estado = last_nonempty("estado_contrato", _ESTADO_KEYS)
    productor = last_nonempty("productor", _PRODUCTOR_KEYS, default="PROMECOR")
    email_trato = last_nonempty("email_del_trato", _EMAIL_TRATO_KEYS)
    costo_mensual = last_nonempty("costo_mensual", _COSTO_KEYS, default=None)

    # ---------- Badges basados SOLO en ÚLTIMO PERÍODO ----------
    # Prioridad: si la columna existe en extra -> se confía SOLO en extra.
    def show_no_contactar() -> bool:
        saw_extra = False
        for it in qs:
            if _has_any_key(it.extra, _NO_CONTACTAR_KEYS):
                saw_extra = True
                if _is_true_strict(_get_from_extra(it.extra, _NO_CONTACTAR_KEYS)):
                    return True
        if saw_extra:
            return False
        for it in qs:
            if _is_true_strict(it.no_contactar):
                return True
        return False

    def show_cliente_importante() -> bool:
        saw_extra = False
        for it in qs:
            if _has_any_key(it.extra, _CLIENTE_IMPORTANTE_KEYS):
                saw_extra = True
                if _is_true_strict(_get_from_extra(it.extra, _CLIENTE_IMPORTANTE_KEYS)):
                    return True
        if saw_extra:
            return False
        for it in qs:
            if _is_true_strict(it.cliente_importante):
                return True
        return False

    def show_premier() -> bool:
        saw_extra = False
        for it in qs:
            if _has_any_key(it.extra, _PREMIER_KEYS):
                saw_extra = True
                v = _get_from_extra(it.extra, _PREMIER_KEYS)
                if (v or "").strip().lower() == "premier":
                    return True
        if saw_extra:
            return False
        for it in qs:
            if (it.premier or "").strip().lower() == "premier":
                return True
        return False

    badges = {
        "no_contactar": show_no_contactar(),
        "premier": show_premier(),
        "cliente_importante": show_cliente_importante(),
    }

    # ---------- Evolución por periodo (suma total por CUIT, todo histórico) ----------
    evol_map: Dict[str, Decimal] = {}
    for it in qs_all:
        if not it.periodo:
            continue
        key = it.periodo.strftime("%m-%Y")
        evol_map[key] = evol_map.get(key, Decimal("0")) + (it.deuda_total or Decimal("0"))

    evol_labels: List[str] = sorted(
        evol_map.keys(),
        key=lambda s: (int(s.split("-")[1]), int(s.split("-")[0]))
    )
    evol_values: List[str] = [str(evol_map[k]) for k in evol_labels]
    evolucion_tabla = [{"periodo": lab, "deuda": _format_ars(Decimal(v))}
                       for lab, v in zip(evol_labels, evol_values)]

    # ---------- Emails enviados ----------
    emails = (EnvioEmailLog.objects
              .filter(cuit=cuit)
              .order_by("-creado_en")
              .values("creado_en", "asunto", "estado", "destinatarios", "error"))

    contexto = {
        "titulo": titulo,
        "badges": badges,
        "cuit": cuit,
        "contrato": contrato_display,
        "aseguradora": aseguradora_display,
        "q_periodos_deudores": q_periodos,
        "estado_contrato": estado,
        "productor": productor,
        "email_trato": email_trato,
        "costo_mensual_fmt": (_format_ars(costo_mensual) if costo_mensual not in (None, "",) else None),

        "evolucion_tabla": evolucion_tabla,
        "evol_labels_json": json.dumps(evol_labels),
        "evol_values_json": json.dumps([float(v) for v in evol_values]),

        "emails": emails,
        "format_ars": _format_ars,
    }
    return render(request, "art_app/art/consulta_detalle.html", contexto)

# -*- coding: utf-8 -*-
from decimal import Decimal
from datetime import date
import re

from django.shortcuts import render
from django.db.models import Sum, Count, Avg

from ..models import ArtDashboardContratoPeriodo


# ---------- Helpers ----------
def _parse_periodo_query(p: str | None) -> date | None:
    """
    Acepta 'MM-YYYY', 'YYYY-MM' o 'YYYY-MM-DD' y devuelve el primer día del mes.
    """
    if not p:
        return None
    s = str(p).strip()
    if re.fullmatch(r"\d{2}-\d{4}", s):  # MM-YYYY
        mm, yyyy = s.split("-")
        return date(int(yyyy), int(mm), 1)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):  # YYYY-MM-DD
        yyyy, mm, _dd = s.split("-")
        return date(int(yyyy), int(mm), 1)
    if re.fullmatch(r"\d{4}-\d{2}", s):  # YYYY-MM
        yyyy, mm = s.split("-")
        return date(int(yyyy), int(mm), 1)
    return None


def _fmt_periodo_yymm(d: date | None) -> str:
    return "" if not d else f"{d.year:04d}-{d.month:02d}"


# ---------- View ----------
def art_analisis(request):
    # 1) Períodos disponibles
    periodos = (ArtDashboardContratoPeriodo.objects
                .order_by("periodo")
                .values_list("periodo", flat=True).distinct())
    periodos = list(periodos)

    if not periodos:
        # No hay datos cargados todavía
        return render(request, "art_app/art/analisis.html", {
            "available_periods": [],
            "selected_period_str": "",
            "kpi": None,
        })

    # 2) Período seleccionado (GET ?periodo=YYYY-MM o MM-YYYY). Default: último.
    periodo_q = request.GET.get("periodo")
    periodo_sel = _parse_periodo_query(periodo_q)
    if not periodo_sel:
        periodo_sel = periodos[-1]  # último disponible

    qs = ArtDashboardContratoPeriodo.objects.filter(periodo=periodo_sel)

    # Definiciones:
    # - Deuda: Q >= 1
    # - Riesgo / Intimados: Q >= 3 (subconjunto de Deuda)
    deuda_qs = qs.filter(q_periodos_deudores__gte=1)
    riesgo_qs = deuda_qs.filter(q_periodos_deudores__gte=3)

    # 3) KPIs (usando deuda_qs y riesgo_qs)
    total_deuda = deuda_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")
    contratos_total = deuda_qs.values("contrato").distinct().count()

    meses_prom = deuda_qs.aggregate(v=Avg("q_periodos_deudores"))["v"] or 0
    monto_riesgo = riesgo_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")
    contratos_riesgo = riesgo_qs.values("contrato").distinct().count()
    pct_riesgo = float(contratos_riesgo) / contratos_total * 100 if contratos_total else 0.0

    # Segmentos (siempre sobre deuda_qs)
    clientes_imp_monto = deuda_qs.filter(cliente_importante=True).aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")
    clientes_imp_count = deuda_qs.filter(cliente_importante=True).values("contrato").distinct().count()

    premier_monto = deuda_qs.filter(premier=True).aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")
    premier_count = deuda_qs.filter(premier=True).values("contrato").distinct().count()

    no_contactar_monto = deuda_qs.filter(no_contactar=True).aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")
    no_contactar_count = deuda_qs.filter(no_contactar=True).values("contrato").distinct().count()

    kpi = {
        "total_deuda": float(total_deuda),
        "contratos_total": int(contratos_total),
        "meses_prom": float(meses_prom),
        "monto_riesgo": float(monto_riesgo),          # INTIMADOS (Q ≥ 3)
        "contratos_riesgo": int(contratos_riesgo),    # INTIMADOS (Q ≥ 3)
        "pct_riesgo": float(round(pct_riesgo, 2)),    # INTIMADOS / contratos con deuda
        "clientes_imp_monto": float(clientes_imp_monto),
        "clientes_imp_count": int(clientes_imp_count),
        "premier_monto": float(premier_monto),
        "premier_count": int(premier_count),
        "no_contactar_monto": float(no_contactar_monto),
        "no_contactar_count": int(no_contactar_count),
    }

    # 4) Datos para gráficos (solapa Período) — usando deuda_qs
    # Distribución por buckets de Q
    bucket_order = ["1", "2", "3", "4-5", "6+"]
    base = {b: {"monto": 0.0, "contratos": 0} for b in bucket_order}
    for r in deuda_qs.values("bucket_q").annotate(
        monto=Sum("deuda_total"),
        contratos=Count("contrato", distinct=True)
    ):
        b = r["bucket_q"] or ""
        if b in base:
            base[b]["monto"] = float(r["monto"] or 0)
            base[b]["contratos"] = int(r["contratos"] or 0)

    chart_buckets = {
        "labels": bucket_order,
        "monto": [base[b]["monto"] for b in bucket_order],
        "contratos": [base[b]["contratos"] for b in bucket_order],
    }

    # Pareto por aseguradora (top 10 + Otros) — sobre deuda_qs
    aseg = list(deuda_qs.values("aseguradora").annotate(monto=Sum("deuda_total")).order_by("-monto"))
    total_m = float(sum(float(x["monto"] or 0) for x in aseg)) or 0.0
    top_n = 10
    labels, montos, acum_pct = [], [], []
    acc = 0.0
    for i, row in enumerate(aseg):
        if i < top_n:
            m = float(row["monto"] or 0)
            labels.append(row["aseguradora"] or "—")
            montos.append(m)
            acc += m
            acum_pct.append(round((acc / total_m) * 100, 2) if total_m else 0.0)
        else:
            break
    if len(aseg) > top_n:
        otros = sum(float(r["monto"] or 0) for r in aseg[top_n:])
        labels.append("Otros")
        montos.append(otros)
        acc += otros
        acum_pct.append(round((acc / total_m) * 100, 2) if total_m else 0.0)

    chart_aseg = {
        "labels": labels,
        "montos": montos,
        "acum_pct": acum_pct,
    }

    # 5) Datos para gráficos (solapa Histórico) — calcula por cada período
    hist_labels = []
    hist_deuda = []
    hist_monto_riesgo = []  # INTIMADOS (Q ≥ 3)
    hist_pct_riesgo = []    # INTIMADOS / contratos con deuda

    for p in periodos:
        per_deuda_qs = ArtDashboardContratoPeriodo.objects.filter(periodo=p, q_periodos_deudores__gte=1)
        deuda_p = per_deuda_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")

        # INTIMADOS = Q >= 3
        riesgo_p_qs = ArtDashboardContratoPeriodo.objects.filter(periodo=p, q_periodos_deudores__gte=3)
        monto_riesgo_p = riesgo_p_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0.00")

        contratos_deuda_p = per_deuda_qs.values("contrato").distinct().count()
        contratos_riesgo_p = riesgo_p_qs.values("contrato").distinct().count()
        pct_riesgo_p = float(contratos_riesgo_p) / contratos_deuda_p * 100 if contratos_deuda_p else 0.0

        hist_labels.append(_fmt_periodo_yymm(p))
        hist_deuda.append(float(deuda_p))
        hist_monto_riesgo.append(float(monto_riesgo_p))
        hist_pct_riesgo.append(round(pct_riesgo_p, 2))

    chart_hist = {
        "labels": hist_labels,
        "deuda": hist_deuda,
        "monto_riesgo": hist_monto_riesgo,
        "pct_riesgo": hist_pct_riesgo,
    }

    # 6) Contexto
    context = {
        "available_periods": [_fmt_periodo_yymm(p) for p in periodos],
        "selected_period_str": _fmt_periodo_yymm(periodo_sel),
        "kpi": kpi,
        "chart_buckets": chart_buckets,
        "chart_aseg": chart_aseg,
        "chart_hist": chart_hist,  # histórico
    }
    return render(request, "art_app/art/analisis.html", context)



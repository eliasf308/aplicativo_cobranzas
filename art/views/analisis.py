# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from django.db.models import Sum, Avg
from django.shortcuts import render

from ..models import ArtDashboardContratoPeriodo


# =========================
# Helpers
# =========================
def _first_day_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def _parse_periodo_query(p: str | None) -> date | None:
    """
    Acepta 'MM-YYYY', 'YYYY-MM' o 'YYYY-MM-DD' y devuelve el primer día del mes.
    """
    if not p:
        return None
    p = p.strip()
    # MM-YYYY
    try:
        if "-" in p and len(p) == 7 and p[2] == "-":
            mm, yyyy = p.split("-")
            return date(int(yyyy), int(mm), 1)
    except Exception:
        pass
    # YYYY-MM
    try:
        if "-" in p and len(p) == 7 and p[4] == "-":
            yyyy, mm = p.split("-")
            return date(int(yyyy), int(mm), 1)
    except Exception:
        pass
    # YYYY-MM-DD
    try:
        dt = datetime.strptime(p, "%Y-%m-%d").date()
        return date(dt.year, dt.month, 1)
    except Exception:
        pass
    return None


def _fmt_periodo_yymm(d: date) -> str:
    return f"{d.month:02d}-{d.year}"


def _bucketize_q(q: Decimal | float | int | None) -> str:
    """
    Devuelve bucket (texto) para Q períodos deudores:
      '1', '2', '3', '4-5', '6+'
    """
    if q is None:
        return "1"
    try:
        v = float(q)
    except Exception:
        v = 0.0
    if v < 2:
        return "1"
    if v < 3:
        return "2"
    if v < 4:
        return "3"
    if v < 6:
        return "4-5"
    return "6+"


def _linreg_slope(values: list[float]) -> float:
    """
    Pendiente m de mínimos cuadrados para x = 0..n-1.
    Devuelve 0 si n < 2.
    """
    n = len(values)
    if n < 2:
        return 0.0
    mean_x = (n - 1) / 2.0
    mean_y = sum(values) / n if n else 0.0
    num = 0.0
    den = 0.0
    for i, y in enumerate(values):
        dx = i - mean_x
        num += dx * (y - mean_y)
        den += dx * dx
    return num / den if den else 0.0


# =========================
# View
# =========================
def art_analisis(request):
    # 1) Periodos disponibles (orden ascendente)
    periodos_qs = (
        ArtDashboardContratoPeriodo.objects
        .order_by("periodo")
        .values_list("periodo", flat=True)
        .distinct()
    )
    periodos = [_first_day_of_month(p) for p in periodos_qs]
    if not periodos:
        context = {
            "available_periods": [],
            "selected_period_str": "",
            "kpi": {"deuda_total": 0, "monto_riesgo": 0, "pct_riesgo": 0, "meses_prom": 0,
                    "mom_pct": None, "trend6m_pct": None, "yoy_pct": None},
            "chart_buckets": {"labels": [], "deuda": [], "contratos": []},
            "chart_aseg": {"labels": [], "deuda": [], "pareto": []},
            "chart_prod_stack": {"labels": [], "datasets": []},
            "chart_aseg_stack": {"labels": [], "datasets": []},
            "chart_aseg_pie": {"labels": [], "deuda": []},
            "chart_hist": {"labels": [], "deuda": [], "monto_riesgo": [], "pct_riesgo": []},
            "chart_hist_prod_share": {"labels": [], "datasets": []},
            "chart_hist_prod_lines": {"labels": [], "datasets": []},
            "chart_hist_aseg_lines": {"labels": [], "datasets": []},
        }
        return render(request, "art_app/art/analisis.html", context)

    # 2) Período seleccionado
    periodo_sel = _parse_periodo_query(request.GET.get("periodo"))
    if not periodo_sel:
        periodo_sel = periodos[-1]  # último disponible

    qs = ArtDashboardContratoPeriodo.objects.filter(periodo=periodo_sel)

    # 3) Definiciones de conjunto
    deuda_qs = qs.filter(q_periodos_deudores__gte=1)   # Deuda = Q >= 1
    riesgo_qs = qs.filter(q_periodos_deudores__gte=3)  # Riesgo/Intimados = Q >= 3

    # 4) KPI principales base
    deuda_total = deuda_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0")
    monto_riesgo = riesgo_qs.aggregate(v=Sum("deuda_total"))["v"] or Decimal("0")
    meses_prom = deuda_qs.aggregate(v=Avg("q_periodos_deudores"))["v"] or Decimal("0")
    pct_riesgo = float(monto_riesgo) / float(deuda_total) * 100 if deuda_total else 0.0

    # 6) Datos para gráficos (solapa Histórico) — los uso para KPIs también
    hist_labels = []
    hist_deuda = []
    hist_monto_riesgo = []
    hist_pct_riesgo = []

    for per in periodos:
        base_qs = ArtDashboardContratoPeriodo.objects.filter(periodo=per)
        deuda = base_qs.filter(q_periodos_deudores__gte=1).aggregate(v=Sum("deuda_total"))["v"] or 0
        riesgo = base_qs.filter(q_periodos_deudores__gte=3).aggregate(v=Sum("deuda_total"))["v"] or 0
        pct = float(riesgo) / float(deuda) * 100 if deuda else 0.0

        hist_labels.append(_fmt_periodo_yymm(per))
        hist_deuda.append(float(deuda))
        hist_monto_riesgo.append(float(riesgo))
        hist_pct_riesgo.append(round(pct, 1))

    chart_hist = {
        "labels": hist_labels,
        "deuda": hist_deuda,
        "monto_riesgo": hist_monto_riesgo,
        "pct_riesgo": hist_pct_riesgo,
    }

    # ===== KPIs nuevos: MoM, Tendencia 6M (%/mes), YoY =====
    mom_pct = None
    trend6m_pct = None
    yoy_pct = None

    try:
        pos = periodos.index(periodo_sel)
    except ValueError:
        pos = len(periodos) - 1

    # MoM
    if pos > 0:
        prev = hist_deuda[pos - 1]
        cur = hist_deuda[pos]
        if prev:
            mom_pct = round((cur - prev) / prev * 100.0, 1)

    # Tendencia 6M (slope sobre últimos hasta 6, expresado en % mensual sobre el promedio de la ventana)
    start = max(0, pos - 5)
    ventana = hist_deuda[start: pos + 1]
    if len(ventana) >= 2:
        m = _linreg_slope(ventana)  # ARS por mes
        mean_win = sum(ventana) / len(ventana) if ventana else 0.0
        trend6m_pct = round((m / mean_win) * 100.0, 2) if mean_win else 0.0

    # YoY
    if pos >= 12:
        prev_y = hist_deuda[pos - 12]
        cur = hist_deuda[pos]
        if prev_y:
            yoy_pct = round((cur - prev_y) / prev_y * 100.0, 1)

    kpi = {
        "deuda_total": float(deuda_total),
        "monto_riesgo": float(monto_riesgo),
        "pct_riesgo": round(pct_riesgo, 1),
        "meses_prom": float(meses_prom),
        # Nuevos
        "mom_pct": mom_pct,            # variación vs mes anterior
        "trend6m_pct": trend6m_pct,    # % mensual (pendiente / promedio ventana * 100)
        "yoy_pct": yoy_pct,            # variación interanual
    }

    # 5) Datos para gráficos (solapa Período)
    # 5.a) Distribución por buckets de Q
    bucket_labels = ["1", "2", "3", "4-5", "6+"]
    bucket_deuda = {b: 0.0 for b in bucket_labels}
    bucket_contratos = {b: 0 for b in bucket_labels}

    for row in deuda_qs.values("q_periodos_deudores", "deuda_total"):
        b = _bucketize_q(row["q_periodos_deudores"])
        bucket_deuda[b] += float(row["deuda_total"] or 0)
        bucket_contratos[b] += 1

    chart_buckets = {
        "labels": bucket_labels,
        "deuda": [bucket_deuda[b] for b in bucket_labels],
        "contratos": [bucket_contratos[b] for b in bucket_labels],
    }

    # 5.b) Pareto por Aseguradora (Top 10 + Otros)
    top_n_aseg = 10
    aseg_rows = (
        deuda_qs.values("aseguradora")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")
    )
    aseg_top = list(aseg_rows[:top_n_aseg])
    labels_aseg = [r["aseguradora"] or "Sin aseguradora" for r in aseg_top]
    deuda_aseg = [float(r["monto"] or 0) for r in aseg_top]

    if aseg_rows.count() > top_n_aseg:
        otros_total = float(aseg_rows[top_n_aseg:].aggregate(v=Sum("monto"))["v"] or 0)
        labels_aseg.append("Otros")
        deuda_aseg.append(otros_total)

    total_aseg = sum(deuda_aseg) or 1.0
    acumulado = 0.0
    pareto = []
    for v in deuda_aseg:
        acumulado += v
        pareto.append(round(acumulado / total_aseg * 100, 1))

    chart_aseg = {
        "labels": labels_aseg,
        "deuda": deuda_aseg,
        "pareto": pareto,
    }

    # 5.c) Barras apiladas por Productor (severidad Q) — Top 10 (EXCLUYE PROMECOR)
    TOP_N_PROD = 10
    bucket_order = ["1", "2", "3", "4-5", "6+"]
    etiquetas_buckets = {
        "1": "Q = 1",
        "2": "Q = 2",
        "3": "Q = 3",
        "4-5": "Q = 4–5",
        "6+": "Q ≥ 6",
    }

    top_prod_rows = (
        deuda_qs.exclude(productor__iexact="PROMECOR")
        .values("productor")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")[:TOP_N_PROD]
    )
    prod_labels = [(r["productor"] if r["productor"] else "Sin productor") for r in top_prod_rows]

    base = {b: {p: 0.0 for p in prod_labels} for b in bucket_order}
    for r in (
        deuda_qs.exclude(productor__iexact="PROMECOR")
        .filter(productor__in=prod_labels)
        .values("productor", "q_periodos_deudores")
        .annotate(monto=Sum("deuda_total"))
    ):
        p = r["productor"] if r["productor"] else "Sin productor"
        b = _bucketize_q(r["q_periodos_deudores"])
        if b in base and p in base[b]:
            base[b][p] += float(r["monto"] or 0)

    datasets_prod = []
    for b in bucket_order:
        datasets_prod.append({
            "type": "bar",
            "label": etiquetas_buckets.get(b, b),
            "data": [base[b][p] for p in prod_labels],
        })

    chart_prod_stack = {
        "labels": prod_labels,
        "datasets": datasets_prod,
    }

    # 5.d) Barras apiladas por Aseguradora (severidad Q) — Top 10
    TOP_N_ASEG_STACK = 10
    top_aseg_stack_rows = (
        deuda_qs.values("aseguradora")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")[:TOP_N_ASEG_STACK]
    )
    aseg_stack_labels = [(r["aseguradora"] if r["aseguradora"] else "Sin aseguradora") for r in top_aseg_stack_rows]

    base_aseg = {b: {a: 0.0 for a in aseg_stack_labels} for b in bucket_order}
    for r in (
        deuda_qs.filter(aseguradora__in=aseg_stack_labels)
        .values("aseguradora", "q_periodos_deudores")
        .annotate(monto=Sum("deuda_total"))
    ):
        a = r["aseguradora"] if r["aseguradora"] else "Sin aseguradora"
        b = _bucketize_q(r["q_periodos_deudores"])
        if b in base_aseg and a in base_aseg[b]:
            base_aseg[b][a] += float(r["monto"] or 0)

    datasets_aseg_stack = []
    for b in bucket_order:
        datasets_aseg_stack.append({
            "type": "bar",
            "label": etiquetas_buckets.get(b, b),
            "data": [base_aseg[b][a] for a in aseg_stack_labels],
        })

    chart_aseg_stack = {
        "labels": aseg_stack_labels,
        "datasets": datasets_aseg_stack,
    }

    # 5.e) Pie por Aseguradora (Top 10 + Otros) en ARS
    TOP_N_ASEG_PIE = 10
    aseg_pie_rows = (
        deuda_qs.values("aseguradora")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")
    )
    aseg_pie_top = list(aseg_pie_rows[:TOP_N_ASEG_PIE])
    labels_aseg_pie = [r["aseguradora"] or "Sin aseguradora" for r in aseg_pie_top]
    values_aseg_pie = [float(r["monto"] or 0) for r in aseg_pie_top]
    if aseg_pie_rows.count() > TOP_N_ASEG_PIE:
        otros_total_pie = float(aseg_pie_rows[TOP_N_ASEG_PIE:].aggregate(v=Sum("monto"))["v"] or 0)
        labels_aseg_pie.append("Otros")
        values_aseg_pie.append(otros_total_pie)
    chart_aseg_pie = {"labels": labels_aseg_pie, "deuda": values_aseg_pie}

    # 6.b) Histórico: Área apilada % por Productor (compatibilidad)
    TOP_N_HIST = 5
    top_hist_rows = (
        ArtDashboardContratoPeriodo.objects
        .filter(q_periodos_deudores__gte=1)
        .values("productor")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")[:TOP_N_HIST]
    )
    top_keys = [r["productor"] for r in top_hist_rows]
    top_labels = [(k if k else "Sin productor") for k in top_keys]

    datasets_hist_share = [
        {"label": lbl, "data": [], "type": "line", "fill": True, "stack": "share", "tension": 0.2}
        for lbl in top_labels
    ]
    datasets_hist_share.append(
        {"label": "Otros", "data": [], "type": "line", "fill": True, "stack": "share", "tension": 0.2}
    )

    for per in periodos:
        per_qs = (
            ArtDashboardContratoPeriodo.objects
            .filter(periodo=per, q_periodos_deudores__gte=1)
            .values("productor")
            .annotate(monto=Sum("deuda_total"))
        )
        per_map = {r["productor"]: float(r["monto"] or 0) for r in per_qs}
        total = sum(per_map.values())
        top_vals = [per_map.get(k, 0.0) for k in top_keys]
        otros_val = max(0.0, total - sum(top_vals))
        if total > 0:
            shares = [v * 100.0 / total for v in top_vals]
            otros_share = otros_val * 100.0 / total
        else:
            shares = [0.0 for _ in top_vals]
            otros_share = 0.0
        for i, s in enumerate(shares):
            datasets_hist_share[i]["data"].append(round(s, 2))
        datasets_hist_share[-1]["data"].append(round(otros_share, 2))

    chart_hist_prod_share = {
        "labels": [_fmt_periodo_yymm(p) for p in periodos],
        "datasets": datasets_hist_share,
    }

    # 6.c) Histórico: Líneas Top 5 por Productor (ARS) — EXCLUYE PROMECOR
    TOP_N_LINES = 5
    top_lines_rows = (
        ArtDashboardContratoPeriodo.objects
        .filter(q_periodos_deudores__gte=1)
        .exclude(productor__iexact="PROMECOR")
        .values("productor")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")[:TOP_N_LINES]
    )
    line_keys = [r["productor"] for r in top_lines_rows]
    line_labels = [(k if k else "Sin productor") for k in line_keys]

    datasets_hist_lines = [
        {"label": lbl, "data": [], "type": "line", "tension": 0.25}
        for lbl in line_labels
    ]

    for per in periodos:
        per_qs = (
            ArtDashboardContratoPeriodo.objects
            .filter(periodo=per, q_periodos_deudores__gte=1, productor__in=line_keys)
            .values("productor")
            .annotate(monto=Sum("deuda_total"))
        )
        per_map = {r["productor"]: float(r["monto"] or 0) for r in per_qs}
        for i, key in enumerate(line_keys):
            datasets_hist_lines[i]["data"].append(per_map.get(key, 0.0))

    chart_hist_prod_lines = {
        "labels": [_fmt_periodo_yymm(p) for p in periodos],
        "datasets": datasets_hist_lines,
    }

    # 6.d) Histórico: Líneas Top 5 por Aseguradora (ARS)
    TOP_N_ASEG_LINES = 5
    top_aseg_lines_rows = (
        ArtDashboardContratoPeriodo.objects
        .filter(q_periodos_deudores__gte=1)
        .values("aseguradora")
        .annotate(monto=Sum("deuda_total"))
        .order_by("-monto")[:TOP_N_ASEG_LINES]
    )
    aseg_line_keys = [r["aseguradora"] for r in top_aseg_lines_rows]
    aseg_line_labels = [(k if k else "Sin aseguradora") for k in aseg_line_keys]

    datasets_hist_aseg_lines = [
        {"label": lbl, "data": [], "type": "line", "tension": 0.25}
        for lbl in aseg_line_labels
    ]

    for per in periodos:
        per_qs = (
            ArtDashboardContratoPeriodo.objects
            .filter(periodo=per, q_periodos_deudores__gte=1, aseguradora__in=aseg_line_keys)
            .values("aseguradora")
            .annotate(monto=Sum("deuda_total"))
        )
        per_map = {r["aseguradora"]: float(r["monto"] or 0) for r in per_qs}
        for i, key in enumerate(aseg_line_keys):
            datasets_hist_aseg_lines[i]["data"].append(per_map.get(key, 0.0))

    chart_hist_aseg_lines = {
        "labels": [_fmt_periodo_yymm(p) for p in periodos],
        "datasets": datasets_hist_aseg_lines,
    }

    # 7) Contexto
    context = {
        "available_periods": [_fmt_periodo_yymm(p) for p in periodos],
        "selected_period_str": _fmt_periodo_yymm(periodo_sel),
        "kpi": kpi,
        "chart_buckets": chart_buckets,
        "chart_aseg": chart_aseg,
        "chart_prod_stack": chart_prod_stack,
        "chart_aseg_stack": chart_aseg_stack,
        "chart_aseg_pie": chart_aseg_pie,
        "chart_hist": chart_hist,
        "chart_hist_prod_share": chart_hist_prod_share,
        "chart_hist_prod_lines": chart_hist_prod_lines,
        "chart_hist_aseg_lines": chart_hist_aseg_lines,
    }
    return render(request, "art_app/art/analisis.html", context)
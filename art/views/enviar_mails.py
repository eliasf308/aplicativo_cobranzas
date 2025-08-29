from __future__ import annotations

import os
from datetime import date
from functools import singledispatch
from typing import Any, Dict, List

import numpy as np
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect, render
from django.utils import timezone

from art.forms import EnviarMailsARTForm
from art.utils import cargar_consolidado
from gestion_cobranzas.models import EnvioDeudaART, ContratoEnviado
from art.tasks import task_enviar_mails  # usamos la tarea de art.tasks

__all__ = ["enviar_mails_art", "envio_estado"]


# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

@singledispatch
def _py(value: Any) -> Any:  # fallback → sin convertir
    return value

@_py.register(np.bool_)
def _(value: np.bool_) -> bool:  # noqa: E305
    return bool(value)

@_py.register(np.integer)
def _(value: np.integer) -> int:  # noqa: E305
    return int(value)

@_py.register(np.floating)
def _(value: np.floating) -> float:  # noqa: E305
    return float(value)

def _reset_session(request: HttpRequest) -> None:
    """Borra todas las claves de sesión usadas en el asistente."""
    for key in ("envio_grupos", "envio_periodo", "envio_hoja"):
        request.session.pop(key, None)

def _model_has_field(model, field_name: str) -> bool:
    return any(getattr(f, "name", None) == field_name for f in model._meta.get_fields())

def _explode_small_groups(grupos_raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Regla de agrupación:
      - Si un e-mail trae 3 o más contratos → se mantiene agrupado (1 mail).
      - Si trae 1 o 2 contratos → NO se agrupa (1 mail por contrato).
    """
    nuevos: List[Dict[str, Any]] = []
    for g in grupos_raw:
        filas = g.get("filas", []) or []
        email = g.get("email") or ""
        if len(filas) >= 3:
            # Se mantiene agrupado
            nuevos.append(g)
        else:
            # Se separa: un group por fila
            for fila in filas:
                q = fila.get("Q periodos deudores", 0) or 0
                try:
                    q = int(q)
                except Exception:
                    q = 0
                nuevos.append({
                    "email": email,
                    "intimado": bool(q >= 3),
                    "filas": [fila],
                })
    return nuevos


# ────────────────────────────────────────────────────────────────────────────────
# Vista principal  «/art/enviar-mails/»
# ────────────────────────────────────────────────────────────────────────────────

@login_required
def enviar_mails_art(request: HttpRequest) -> HttpResponse | JsonResponse:
    """
    Asistente para el envío de correos de deuda ART.

    Flujo:
      1) Form inicial → agrupa contratos por e-mail (con regla de 3+) y guarda en sesión.
      2) GET con datos en sesión → pantalla de confirmación.
      3) POST «confirmar» → crea registros, encola tarea Celery y limpia sesión.
      4) Botón «Volver» usa ?reset=1 para reiniciar el asistente.
    """

    # ───────────── Botón «Volver» (reinicia) ─────────────
    if request.GET.get("reset") == "1":
        _reset_session(request)
        return redirect("art:art_enviar_mails")

    # ========================================================================
    # PASO 2 — Confirmación y *envío*  (POST desde AJAX o submit normal)
    # ========================================================================
    if request.method == "POST" and (
        "confirmar" in request.POST
        or request.headers.get("X-Requested-With") == "XMLHttpRequest"
    ):
        grupos: list[Dict[str, Any]] | None = request.session.get("envio_grupos")
        periodo: str | None = request.session.get("envio_periodo")  # «MM/AAAA»
        hoja: str | None = request.session.get("envio_hoja")

        if not grupos or not periodo or not hoja:
            messages.error(request, "La sesión expiró. Volvé a empezar.")
            return redirect("art:art_enviar_mails")

        mes, anio = map(int, periodo.split("/"))
        fecha_arch = date(anio, mes, 1)
        envios_ids: list[int] = []

        # Alias por defecto (si el modelo tiene campo desde_cuenta)
        default_alias = os.getenv("GMAIL_SENDER_ALIAS", "florencia").strip().lower()
        include_desde_cuenta = _model_has_field(EnvioDeudaART, "desde_cuenta")

        for g in grupos:
            # 1) Cabecera EnvioDeudaART
            envio_kwargs = dict(
                fecha_archivo=fecha_arch,
                hoja=hoja,
                email=g["email"],
                subject="",                  # la tarea Celery lo completa
                enviado_por=request.user,
                fecha_envio=timezone.now(),  # marca de encolado
            )
            if include_desde_cuenta:
                envio_kwargs["desde_cuenta"] = default_alias

            envio = EnvioDeudaART.objects.create(**envio_kwargs)
            envios_ids.append(envio.id)

            # 2) Detalle de contratos (badge INTIMADO por fila según q_periodos)
            ContratoEnviado.objects.bulk_create(
                [
                    ContratoEnviado(
                        envio=envio,
                        contrato=fila["Contrato"],
                        razon_social=fila["Razón social"],
                        cuit=fila["CUIT"],
                        aseguradora=fila["Aseguradora"],
                        deuda_total=fila["Deuda total"],
                        q_periodos=fila["Q periodos deudores"],
                        intimado=(fila["Q periodos deudores"] >= 3),
                    )
                    for fila in g["filas"]
                ]
            )

        # 3) Encolar envío asíncrono (acepta lista o entero)
        task_enviar_mails.delay(envios_ids)

        # 4) Limpiar sesión y responder
        _reset_session(request)

        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return JsonResponse({"ids": envios_ids, "total": len(envios_ids)})

        messages.success(request, "Los correos fueron encolados correctamente.")
        return redirect("art:art_enviar_mails")

    # ========================================================================
    # PASO 1 — Procesar formulario inicial  (POST sin «confirmar»)
    # ========================================================================
    if request.method == "POST":
        form = EnviarMailsARTForm(request.POST)
        if form.is_valid():
            periodo = form.cleaned_data["fecha"].strftime("%m/%Y")
            hoja = form.cleaned_data["hoja"]

            # Leer Excel y agrupar por e-mail (art.utils.cargar_consolidado)
            try:
                grupos_raw = cargar_consolidado(periodo, hoja)
            except Exception as exc:  # noqa: BLE001
                messages.error(request, str(exc))
                return render(request, "art_app/art/enviar_mails.html", {"form": form})

            # Aplicar regla de agrupación (solo agrupar si ≥3 contratos)
            grupos_raw = _explode_small_groups(grupos_raw)

            # Convertir tipos NumPy → JSON-safe (para guardar en sesión)
            grupos: list[Dict[str, Any]] = []
            for g in grupos_raw:
                filas_conv = [{k: _py(v) for k, v in fila.items()} for fila in g["filas"]]
                # recalcular intimado a nivel "grupo" por si vino mal en origen
                q0 = filas_conv[0].get("Q periodos deudores", 0) if filas_conv else 0
                try:
                    q0 = int(q0)
                except Exception:
                    q0 = 0
                grupos.append({
                    "email": g["email"],
                    "intimado": bool(q0 >= 3),
                    "filas": filas_conv,
                })

            # Guardar en sesión y recargar (GET) para la confirmación
            request.session["envio_grupos"] = grupos
            request.session["envio_periodo"] = periodo
            request.session["envio_hoja"] = hoja
            return redirect("art:art_enviar_mails")
    else:
        form = EnviarMailsARTForm()

    # ========================================================================
    # GET — ¿hay datos en sesión? → mostrar pantalla de confirmación
    # ========================================================================
    grupos = request.session.get("envio_grupos")
    if grupos:
        ctx = {
            "periodo": request.session["envio_periodo"],
            "hoja": request.session["envio_hoja"],
            "grupos": grupos,
        }
        return render(request, "art_app/art/enviar_mails_confirm.html", ctx)

    # ========================================================================
    # GET — formulario inicial
    # ========================================================================
    return render(request, "art_app/art/enviar_mails.html", {"form": form})


# ────────────────────────────────────────────────────────────────────────────────
# Endpoint AJAX  «/art/envio-estado/?ids=…»  (progreso de Celery)
# ────────────────────────────────────────────────────────────────────────────────

@login_required
def envio_estado(request: HttpRequest) -> JsonResponse:
    ids_raw = request.GET.get("ids", "")
    try:
        ids_int = [int(i) for i in ids_raw.split(",") if i]
    except ValueError:
        return JsonResponse({"error": "IDs incorrectos"}, status=400)

    qs = EnvioDeudaART.objects.filter(id__in=ids_int).values(
        "id",
        "enviado_ok",
        "enviado_error",
    )
    return JsonResponse({"envios": list(qs)}, safe=False)

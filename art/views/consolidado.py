# art/views/consolidado.py
# -*- coding: utf-8 -*-
"""
Vista que genera y permite descargar el Consolidado de Deudas ART
(en un único XLSX con 10 hojas: Consolidado, No cruzan, Sin mail, etc.).
Luego de guardar el lote+items en BD, actualiza el panel (ArtDashboardContratoPeriodo).
"""

from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.management import call_command
from django.http import FileResponse, HttpRequest
from django.shortcuts import render, redirect
from django.utils.encoding import iri_to_uri

import pandas as pd

# ⬇️  API que genera el XLSX en memoria
from art.services.consolidar import generar_xlsx
# ⬇️  Servicio que persiste el lote + items en BD
from art.services.persistencia_consolidado import guardar_lote_y_items


@login_required
def consolidado_view(request: HttpRequest):
    """
    GET  → muestra un formulario con <input type="month" name="periodo"> (ej. 2025-06).
    POST → genera el XLSX completo y lo descarga. Además guarda la corrida en BD
           y vuelca el período al tablero de análisis.
    """
    if request.method == "POST":
        yyyy_mm = request.POST.get("periodo")  # p. ej. "2025-06"
        if not yyyy_mm:
            return redirect(request.path)

        # 1) Normalizar para ambas cosas:
        #    - periodo (MM-YYYY) → nombre de archivo, generación de XLSX y persistencia
        #    - periodo_for_cmd (YYYY-MM) → comando de volcado del panel
        try:
            yyyy, mm = yyyy_mm.split("-")
            periodo = f"{mm}-{yyyy}"          # "06-2025"
            periodo_for_cmd = f"{yyyy}-{mm}"  # "2025-06"
        except ValueError:
            return redirect(request.path)

        # 2) Generar el XLSX (BytesIO) con todas las hojas
        buffer = generar_xlsx(periodo)  # ← función existente

        # 3) Leer hojas clave directamente desde el buffer (sin escribir a disco) y guardar en BD
        try:
            buffer.seek(0)
            xls = pd.ExcelFile(buffer)

            def _read_if_exists(nombre_hoja: str):
                return (
                    pd.read_excel(xls, sheet_name=nombre_hoja)
                    if nombre_hoja in xls.sheet_names
                    else None
                )

            df_consolidado = _read_if_exists("Consolidado")
            df_no_cruzan = _read_if_exists("No cruzan")
            df_productor = _read_if_exists("Productor")

            # Guardar en base de datos (lote + items) reemplazando el período
            guardar_lote_y_items(
                usuario=request.user,
                periodo_str=periodo,                  # "MM-YYYY"
                df_consolidado=df_consolidado,
                df_no_cruzan=df_no_cruzan,
                df_productor=df_productor,
                nombre_archivo_maestro="",            # opcional: completá si lo tenés
                archivos_fuente={},                   # opcional
                ruta_excel_salida=f"Consolidado_ART_{periodo}.xlsx",
                observaciones="Guardado automático desde consolidado_view.",
                reemplazar_periodo=True,              # ⬅️ borra e inserta ese período
            )

            # 4) Volcar al tablero (ArtDashboardContratoPeriodo) — idempotente
            try:
                call_command("volcar_dashboard_art", periodo=periodo_for_cmd, reset_periodo=True)
                messages.success(request, f"Panel actualizado para {periodo_for_cmd}.")
            except Exception as e:
                # No bloquea la descarga si falla el volcado
                messages.warning(request, f"Consolidado OK pero no pude actualizar el panel: {e}")

        except Exception as e:
            # Si algo falla en la persistencia NO bloqueamos la descarga.
            # Podés loguear el error si querés:
            # import logging; logging.getLogger(__name__).exception("Error guardando consolidado")
            messages.warning(request, f"El consolidado se descargará, pero no pude guardar el lote/items: {e}")

        # 5) Preparar respuesta de descarga
        buffer.seek(0)
        filename = iri_to_uri(f"Consolidado_ART_{periodo}.xlsx")
        return FileResponse(
            buffer,
            as_attachment=True,
            filename=filename,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # GET → renderiza formulario
    return render(request, "art_app/art/generar_archivo.html")


from gestion_cobranzas.models import EnvioDeudaART, ContratoEnviado
from django.contrib import admin
from .models import ConsolidadoLote, ConsolidadoItem, EnvioEmailLog, ConsolidadoArt

@admin.register(ConsolidadoLote)
class ConsolidadoLoteAdmin(admin.ModelAdmin):
    list_display = ("id", "creado_en", "usuario", "filas_consolidado", "filas_no_cruzan")
    search_fields = ("id", "usuario__username", "nombre_archivo_maestro")
    date_hierarchy = "creado_en"
    ordering = ("-creado_en",)

@admin.register(ConsolidadoItem)
class ConsolidadoItemAdmin(admin.ModelAdmin):
    list_display = ("id", "lote", "periodo", "cuit", "razon_social", "aseguradora",
                    "contrato", "deuda_total", "hoja")
    list_filter = ("hoja", "aseguradora", "periodo")
    search_fields = ("cuit", "razon_social", "contrato", "aseguradora", "estado_contrato")
    autocomplete_fields = ("lote",)
    ordering = ("-periodo", "-deuda_total")

@admin.register(EnvioEmailLog)
class EnvioEmailLogAdmin(admin.ModelAdmin):
    list_display = ("id", "creado_en", "cuit", "asunto", "estado")
    list_filter = ("estado",)
    search_fields = ("cuit", "asunto", "destinatarios")
    date_hierarchy = "creado_en"
    ordering = ("-creado_en",)

@admin.register(ConsolidadoArt)
class ConsolidadoArtAdmin(admin.ModelAdmin):
    list_display = ("periodo", "cuit", "aseguradora", "razon_social", "deuda_total")
    list_filter = ("periodo", "aseguradora")
    search_fields = ("cuit", "razon_social")
    ordering = ("-periodo",)
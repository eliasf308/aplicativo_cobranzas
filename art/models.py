# art/models.py
from django.db import models
from django.contrib.auth import get_user_model
from decimal import Decimal

User = get_user_model()


# =========================
# 1) SNAPSHOT POR CORRIDA
# =========================
class ConsolidadoLote(models.Model):
    """
    Cabecera de una ejecución del consolidado (auditoría y metadatos).
    """
    creado_en = models.DateTimeField(auto_now_add=True)
    usuario = models.ForeignKey(
        User, on_delete=models.PROTECT, related_name="lotes_consolidado"
    )

    # Auditoría de entradas/salida
    nombre_archivo_maestro = models.CharField(max_length=255, blank=True)
    archivos_fuente = models.JSONField(default=dict, blank=True)   # {"Andina": "06-2025.xlsx", ...}
    ruta_excel_salida = models.CharField(max_length=500, blank=True)

    # Resumen del proceso
    filas_consolidado = models.PositiveIntegerField(default=0)
    filas_no_cruzan = models.PositiveIntegerField(default=0)

    # Para deduplicar si hiciera falta (hash de insumos)
    hash_entrada = models.CharField(max_length=64, blank=True, db_index=True)

    observaciones = models.TextField(blank=True)

    class Meta:
        ordering = ["-creado_en"]
        db_table = "art_consolidado_lote"

    def __str__(self):
        return f"Lote #{self.id} - {self.creado_en:%Y-%m-%d %H:%M}"


class ConsolidadoItem(models.Model):
    """
    Detalle por fila resultante del consolidado, vinculado a un Lote.
    Incluye todos los campos necesarios para la vista 'Consulta' y para evolución por periodo.
    """
    HOJA_CHOICES = [
        ("consolidado", "Consolidado"),
        ("no_cruzan", "No cruzan"),
        ("productor", "Productor"),
    ]

    lote = models.ForeignKey(
        ConsolidadoLote, on_delete=models.CASCADE, related_name="items"
    )

    # Claves de consulta
    cuit = models.CharField(max_length=20, db_index=True)
    # Convención: primer día del mes del periodo (ej. 2025-06-01)
    periodo = models.DateField(null=True)

    # Datos de contexto (para la vista Consulta)
    razon_social = models.CharField(max_length=255, blank=True)
    aseguradora = models.CharField(max_length=120, db_index=True)
    # Guardamos contrato como texto para evitar problemas de formato/longitud entre aseguradoras
    contrato = models.CharField(max_length=60, blank=True)

    deuda_total = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    costo_mensual = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    # Lo dejamos decimal para respetar el shape de tu consolidado actual
    q_periodos_deudores = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)

    estado_contrato = models.CharField(max_length=60, blank=True)
    email_del_trato = models.CharField(max_length=255, blank=True)
    no_contactar = models.BooleanField(default=False)

    productor = models.CharField(max_length=120, default="PROMECOR")
    PREMIER_CHOICES = (("Premier", "Premier"), ("No es Premier", "No es Premier"))
    premier = models.CharField(max_length=20, choices=PREMIER_CHOICES, blank=True, default="No es Premier")

    cliente_importante = models.BooleanField(default=False)

    en_deuda = models.BooleanField(default=True)  # redundante pero útil para filtros rápidos
    hoja = models.CharField(max_length=20, choices=HOJA_CHOICES, default="consolidado")

    # Bolsa flexible para columnas específicas de alguna aseguradora
    extra = models.JSONField(default=dict, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["cuit", "periodo"]),
            models.Index(fields=["aseguradora"]),
            models.Index(fields=["hoja"]),
        ]
        unique_together = (
            # Evita duplicar exactamente la misma fila dentro del mismo lote
            ("lote", "cuit", "aseguradora", "contrato", "periodo", "hoja"),
        )
        db_table = "art_consolidado_item"

    def __str__(self):
        per = self.periodo.strftime("%Y-%m") if self.periodo else "s/periodo"
        return f"{self.cuit} - {self.aseguradora} ({self.hoja}) {per}"


# =========================
# 2) LOG DE ENVÍOS DE MAIL
# =========================
class EnvioEmailLog(models.Model):
    """
    Registro de cada email de deuda (o intento), para cruzar por CUIT en 'Consulta'.
    """
    ESTADOS = [
        ("enviado", "Enviado"),
        ("fallido", "Fallido"),
        ("excluido", "Excluido"),
    ]

    creado_en = models.DateTimeField(auto_now_add=True)
    usuario = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, related_name="envios_email"
    )

    cuit = models.CharField(max_length=20, db_index=True)
    aseguradora = models.CharField(max_length=120, blank=True)
    contrato = models.CharField(max_length=60, blank=True)

    destinatarios = models.JSONField(default=list, blank=True)  # lista de emails a los que se envió
    asunto = models.CharField(max_length=255)
    cuerpo_resumen = models.TextField(blank=True)               # snapshot del cuerpo (texto plano)
    estado = models.CharField(max_length=10, choices=ESTADOS, default="enviado")
    error = models.TextField(blank=True)
    metadata = models.JSONField(default=dict, blank=True)       # message_id, thread_id, etc.

    # Envío asociado a un consolidado (opcional)
    lote_consolidado = models.ForeignKey(
        ConsolidadoLote, on_delete=models.SET_NULL, null=True, blank=True
    )

    class Meta:
        ordering = ["-creado_en"]
        indexes = [
            models.Index(fields=["cuit", "creado_en"]),
            models.Index(fields=["estado"]),
        ]
        db_table = "art_envio_email_log"

    def __str__(self):
        return f"[{self.estado.upper()}] {self.cuit} - {self.asunto}"


# =========================
# 3) TABLA EXISTENTE (compatibilidad)
# =========================
class ConsolidadoArt(models.Model):
    """
    Una fila por combinación (periodo, cuit, aseguradora).
    Refleja 1:1 las columnas de la hoja "Consolidado".
    Se mantiene para compatibilidad y consultas rápidas por periodo.
    """
    # claves / dimensiones
    periodo = models.CharField(max_length=7, db_index=True)        # 'MM-AAAA'
    cuit = models.CharField(max_length=11, db_index=True)          # 11 dígitos normalizados
    aseguradora = models.CharField(max_length=120, db_index=True)

    # datos principales
    razon_social = models.CharField(max_length=255)
    contrato = models.BigIntegerField(null=True, blank=True)

    deuda_total = models.DecimalField(max_digits=14, decimal_places=2)                  # admite negativos
    costo_mensual = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    q_periodos_deudores = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)

    estado_contrato = models.CharField(max_length=60)
    email_del_trato = models.CharField(max_length=255, null=True, blank=True)
    no_contactar = models.BooleanField(default=False)

    productor = models.CharField(max_length=120, default="PROMECOR")
    PREMIER_CHOICES = (("Premier", "Premier"), ("No es Premier", "No es Premier"))
    premier = models.CharField(max_length=20, choices=PREMIER_CHOICES)

    cliente_importante = models.BooleanField(default=False)

    # housekeeping
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "art_consolidado"
        unique_together = ("periodo", "cuit", "aseguradora")
        indexes = [
            models.Index(fields=["periodo", "aseguradora"]),
            models.Index(fields=["periodo", "cuit"]),
        ]

    def __str__(self):
        return f"{self.periodo} | {self.cuit} | {self.aseguradora}"

class ArtDashboardContratoPeriodo(models.Model):
    """
    Tabla 'hecho' para el panel de análisis ART:
    1 fila = 1 contrato en un período (mes).
    """
    periodo = models.DateField(help_text="Primer día del mes del consolidado (ej: 2025-06-01).")

    # Dimensiones base
    razon_social = models.CharField(max_length=200, blank=True, default="")
    cuit = models.CharField(max_length=20, db_index=True)
    contrato = models.CharField(max_length=50, db_index=True)
    aseguradora = models.CharField(max_length=120, db_index=True)

    # Métricas
    deuda_total = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))
    costo_mensual = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)

    # Severidad
    q_periodos_deudores = models.DecimalField(max_digits=6, decimal_places=2, null=True, blank=True)
    estado_contrato = models.CharField(max_length=60, blank=True, default="")

    # Contactabilidad / segmentos
    email_trato = models.CharField(max_length=200, blank=True, default="")
    no_contactar = models.BooleanField(default=False)
    productor = models.CharField(max_length=120, blank=True, default="")
    premier = models.BooleanField(default=False)
    cliente_importante = models.BooleanField(default=False)

    # Derivados para el dashboard
    riesgo_flag = models.BooleanField(default=False)  # Q >= 2
    bucket_q = models.CharField(
        max_length=10,
        blank=True,
        default="",
        help_text="Buckets: '1', '2', '3', '4-5', '6+'"
    )
    deuda_vs_costo = models.DecimalField(
        max_digits=18, decimal_places=4, null=True, blank=True,
        help_text="deuda_total / costo_mensual (si costo > 0)"
    )

    # Opcional para trazabilidad
    lote_ref = models.CharField(max_length=100, blank=True, default="", help_text="Identificador de lote/origen")

    class Meta:
        verbose_name = "ART Dashboard (Contrato-Período)"
        verbose_name_plural = "ART Dashboard (Contrato-Período)"
        constraints = [
            models.UniqueConstraint(
                fields=["periodo", "cuit", "contrato", "aseguradora"],
                name="uniq_art_dash_periodo_cuit_contrato_aseguradora",
            )
        ]
        indexes = [
            models.Index(fields=["periodo"]),
            models.Index(fields=["aseguradora"]),
            models.Index(fields=["productor"]),
            models.Index(fields=["riesgo_flag", "aseguradora"]),
            models.Index(fields=["bucket_q"]),
            models.Index(fields=["premier"]),
            models.Index(fields=["cliente_importante"]),
        ]

    def __str__(self):
        return f"{self.periodo} | {self.aseguradora} | {self.contrato} | {self.cuit}"
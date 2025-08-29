from __future__ import annotations

from decimal import Decimal
from django.contrib.auth import get_user_model
from django.db import models
from django.utils import timezone


# ────────────────────────────────────────────────────────────────
# MODELOS BASE  (sin cambios respecto a tu versión)
# ────────────────────────────────────────────────────────────────
class Aseguradora(models.Model):
    nombre = models.CharField(max_length=100, unique=True)

    def __str__(self) -> str:
        return self.nombre


class Ramo(models.Model):
    nombre = models.CharField(max_length=100, unique=True)

    def __str__(self) -> str:
        return self.nombre


class Poliza(models.Model):
    numero = models.CharField(max_length=100)
    aseguradora = models.ForeignKey(Aseguradora, on_delete=models.CASCADE)
    ramo = models.ForeignKey(Ramo, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("numero", "aseguradora", "ramo")

    def __str__(self) -> str:
        return f"{self.numero} ({self.aseguradora})"


class PlanPago(models.Model):
    MONEDAS = [
        ("$", "Pesos"),
        ("U$S", "Dólares"),
    ]

    aseguradora = models.ForeignKey(Aseguradora, on_delete=models.CASCADE)
    ramo = models.ForeignKey(Ramo, on_delete=models.CASCADE)
    poliza = models.ForeignKey(Poliza, on_delete=models.CASCADE)
    endoso = models.CharField(max_length=20)
    moneda = models.CharField(max_length=4, choices=MONEDAS, default="$")

    def __str__(self) -> str:
        return f"{self.poliza.numero}-{self.endoso}"


class Cuota(models.Model):
    plan_pago = models.ForeignKey(
        PlanPago, on_delete=models.CASCADE, related_name="cuotas"
    )
    numero = models.PositiveIntegerField()
    vencimiento = models.DateField()
    importe = models.DecimalField(max_digits=10, decimal_places=2)
    importe_original = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    monto_imputado = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True, default=0
    )

    def __str__(self) -> str:
        return f"Cuota {self.numero} – Vence {self.vencimiento} – ${self.importe}"


# ────────────────────────────────────────────────────────────────
# LOGS (sin cambios)
# ────────────────────────────────────────────────────────────────
class LogCargaMasiva(models.Model):
    usuario = models.ForeignKey(get_user_model(), on_delete=models.SET_NULL, null=True)
    fecha_carga = models.DateTimeField(auto_now_add=True)
    aseguradora = models.CharField(max_length=100)
    ramo = models.CharField(max_length=100)
    poliza = models.CharField(max_length=100)
    endoso = models.CharField(max_length=100)
    cantidad_cuotas = models.PositiveIntegerField()
    archivo = models.CharField(max_length=255)

    def __str__(self) -> str:
        return (
            f"{self.fecha_carga:%Y-%m-%d %H:%M} – "
            f"{self.usuario} – {self.poliza}-{self.endoso}"
        )


class LogImputacion(models.Model):
    usuario = models.ForeignKey(get_user_model(), on_delete=models.SET_NULL, null=True)
    fecha_carga = models.DateTimeField(auto_now_add=True)
    archivo = models.CharField(max_length=255)
    cantidad_cuotas_imputadas = models.PositiveIntegerField()

    def __str__(self) -> str:
        return f"{self.fecha_carga:%Y-%m-%d %H:%M} – {self.usuario} – {self.archivo}"


# ────────────────────────────────────────────────────────────────
# 📵  BLOQUEO DE CORREOS
# ────────────────────────────────────────────────────────────────
class BlocklistEmail(models.Model):
    """
    Correos que NO deben recibir notificaciones.
    """
    email = models.EmailField(unique=True)
    motivo = models.CharField(max_length=200, blank=True)

    class Meta:
        db_table = "gestion_cobranzas_blocklistemail"

    def __str__(self) -> str:
        return self.email


# ────────────────────────────────────────────────────────────────
# ENVÍO DE CORREOS DEUDA ART  (única versión en esta app)
# ────────────────────────────────────────────────────────────────
class EnvioDeudaART(models.Model):
    ESTADOS = [
        ("PENDIENTE", "Pendiente"),
        ("ENVIADO", "Enviado"),
        ("ERROR", "Error"),
    ]

    fecha_archivo = models.DateField(help_text="Mes/año del Excel (01-2025)")
    hoja = models.CharField(max_length=20)
    email = models.EmailField()
    subject = models.CharField(max_length=200, blank=True)
    enviado_por = models.ForeignKey(
        get_user_model(),
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="envios_deuda_art",  # evita choque con otras apps
    )
    fecha_envio = models.DateTimeField(default=timezone.now)
    estado = models.CharField(max_length=10, choices=ESTADOS, default="PENDIENTE")
    detalle_error = models.TextField(blank=True)
    message_id = models.CharField(max_length=200, blank=True)

    class Meta:
        db_table = "gestion_cobranzas_enviodeudaart"
        ordering = ["-fecha_envio"]

    def __str__(self) -> str:
        return f"{self.email} – {self.fecha_archivo:%m/%Y}"


class ContratoEnviado(models.Model):
    envio = models.ForeignKey(
        EnvioDeudaART,
        on_delete=models.CASCADE,
        related_name="contratos",
    )
    contrato = models.CharField(max_length=50)
    razon_social = models.CharField(max_length=120)
    cuit = models.CharField(max_length=20)
    aseguradora = models.CharField(max_length=60)
    deuda_total = models.DecimalField(
        max_digits=12, decimal_places=2, default=Decimal("0.00")
    )
    q_periodos = models.PositiveIntegerField()
    intimado = models.BooleanField(default=False)

    class Meta:
        db_table = "gestion_cobranzas_contratoenviado"
        ordering = ["contrato"]

    def __str__(self) -> str:
        return f"{self.contrato} – ${self.deuda_total:,.2f}"

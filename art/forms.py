from django import forms
from django.forms import modelformset_factory

from gestion_cobranzas.models import PlanPago, Cuota



# ───────────────────────────────────────────────────────────────
# Carga masiva de planes (existente)
# ───────────────────────────────────────────────────────────────
class CargaMasivaForm(forms.Form):
    archivo = forms.FileField(label="Seleccioná un archivo Excel")


class PlanPagoForm(forms.ModelForm):
    class Meta:
        model = PlanPago
        fields = ["aseguradora", "ramo", "poliza", "endoso"]
        widgets = {
            "endoso": forms.TextInput(attrs={"class": "form-control"}),
            "aseguradora": forms.Select(attrs={"class": "form-select"}),
            "ramo": forms.Select(attrs={"class": "form-select"}),
            "poliza": forms.Select(attrs={"class": "form-select"}),
        }


class CuotaForm(forms.ModelForm):
    class Meta:
        model = Cuota
        fields = ["numero", "vencimiento", "importe"]
        widgets = {
            "numero": forms.NumberInput(attrs={"class": "form-control"}),
            "vencimiento": forms.DateInput(
                attrs={"type": "date", "class": "form-control"}
            ),
            "importe": forms.NumberInput(attrs={"class": "form-control"}),
        }


# Formset para editar todas las cuotas juntas
CuotaFormSet = modelformset_factory(Cuota, form=CuotaForm, extra=0)


class ImputacionExcelForm(forms.Form):
    archivo = forms.FileField(label="Archivo Excel de Pagos")


# ───────────────────────────────────────────────────────────────
# NUEVO · Enviar mails ART
# ───────────────────────────────────────────────────────────────
class EnviarMailsARTForm(forms.Form):
    HOJA_CHOICES = [
        ("Deuda Promecor", "Deuda Promecor"),
        ("Productor", "Productor"),
    ]

    fecha = forms.DateField(
        label="Mes a procesar",
        input_formats=["%Y-%m"],                      # ← clave
        widget=forms.DateInput(
            attrs={"type": "month", "class": "form-control"},
        ),
        help_text="Elegí mes/año (por ejemplo 2025-07)",
    )

    hoja = forms.ChoiceField(
        label="Hoja",
        choices=HOJA_CHOICES,
        widget=forms.Select(attrs={"class": "form-select"}),
    )
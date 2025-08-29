from django.contrib import admin
from .models import Aseguradora, Ramo, Poliza, PlanPago

admin.site.register(Aseguradora)
admin.site.register(Ramo)
admin.site.register(Poliza)
admin.site.register(PlanPago)

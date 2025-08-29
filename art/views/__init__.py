"""
art.views
~~~~~~~~~
Reexporta todas las vistas públicas del módulo ART.
"""

from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect

# --- Vistas individuales --------------------------------------------------
from .consolidado import consolidado_view as art_generar_archivo
from .enviar_mails import enviar_mails_art, envio_estado

@login_required
def art_home(request):
    
    return redirect("art:art_generar_archivo")

__all__ = [
    "art_home",
    "art_generar_archivo",
    "enviar_mails_art",
    "envio_estado",
]

# gestion_cobranzas/tasks.py
from celery import shared_task
from django.core.mail import send_mail
from gestion_cobranzas.models import EnvioDeudaART


@shared_task
def task_enviar_mails(envios_ids: list[int]):
    """
    Recorre los IDs recibidos, arma el correo y lo envía.
    """
    for envio in EnvioDeudaART.objects.filter(id__in=envios_ids):
        # construir subject y body…
        send_mail(
            subject=envio.subject or "Deuda ART",
            message="Cuerpo…",
            from_email="cobranzas@…",
            recipient_list=[envio.email],
        )
        envio.enviado_ok = True
        envio.save(update_fields=["enviado_ok"])

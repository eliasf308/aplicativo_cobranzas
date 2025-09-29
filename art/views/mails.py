from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin

class EnviarMailsFormView(LoginRequiredMixin, TemplateView):
    template_name = "art/enviar_mails/form.html"


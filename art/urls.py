# art/urls.py
from django.urls import path
import art.views as art_views
from art.views.consulta import consulta_busqueda_view, consulta_detalle_view  
from .views.analisis import art_analisis      

app_name = "art"

urlpatterns = [
    path("",               art_views.art_home,           name="art_home"),
    path("generar-archivo/", art_views.art_generar_archivo, name="art_generar_archivo"),
    path("enviar-mails/", art_views.enviar_mails_art, name="art_enviar_mails"),
    path("envio-estado/",    art_views.envio_estado,        name="envio_estado"),
    path("consulta/", consulta_busqueda_view, name="consulta_busqueda"),
    path("consulta/<str:cuit>/", consulta_detalle_view, name="consulta_detalle"),
    path("analisis/", art_analisis, name="art_analisis"),
]

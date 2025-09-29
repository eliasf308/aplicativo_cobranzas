from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    # Admin
    path('admin/', admin.site.urls),

    # Landing pública + dashboard privado (namespace core)
    path('', include(('core.urls', 'core'), namespace='core')),

    # EXponer gestión_cobranzas en la raíz para que funcionen /planes/... del sidebar
    path('', include('gestion_cobranzas.urls')),

    # (Opcional) mantener también el prefijo /app/ si lo estás usando en algún botón
    path('app/', include('gestion_cobranzas.urls')),

    # Módulo ART
    path('art/', include('art.urls')),

    # Autenticación
    path('accounts/', include('django.contrib.auth.urls')),
]

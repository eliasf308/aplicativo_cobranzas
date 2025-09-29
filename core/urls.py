from django.urls import path
from .views import home_public, dashboard

app_name = "core"

urlpatterns = [
    path('', home_public, name='home_public'),   # Landing pública
    path('dashboard/', dashboard, name='home'),  # Panel privado (coincide con LOGIN_REDIRECT_URL)
]

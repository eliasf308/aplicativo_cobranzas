from django.shortcuts import render
from django.contrib.auth.decorators import login_required


def home_public(request):
    """
    Landing pública. No requiere autenticación.
    """
    return render(request, 'core/home_public.html')


@login_required
def dashboard(request):
    """
    Panel privado (post-login). Requiere usuario autenticado.
    """
    return render(request, 'core/dashboard.html')

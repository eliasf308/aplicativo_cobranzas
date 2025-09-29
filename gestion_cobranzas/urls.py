from django.urls import path, include
from . import views

urlpatterns = [
    # ───────── SEG. GENERALES ─────────
    path('planes/carga-excel/',         views.cargar_planes_excel,  name='cargar_planes_excel'),
    path('planes/listado/',             views.listar_planes,        name='listar_planes'),
    path('planes/<int:plan_id>/cuotas/', views.ver_cuotas_plan,     name='ver_cuotas_plan'),
    path('planes/<int:plan_id>/eliminar/', views.eliminar_plan,     name='eliminar_plan'),
    path('planes/<int:plan_id>/editar/',   views.editar_plan,       name='editar_plan'),
    path('planes/<int:plan_id>/cuotas/editar/', views.editar_cuotas_plan, name='editar_cuotas_plan'),
    path('planes/imputar-excel/',       views.imputar_pagos_excel,  name='imputar_pagos_excel'),
    path('planes/exportar/',            views.exportar_planes_excel,name='exportar_planes_excel'),
    path('reportes/',                   views.reportes,             name='reportes'),
    path('planes/<int:plan_id>/exportar-excel/', views.exportar_excel_cuotas, name='exportar_excel_cuotas'),
    path('planes/<int:plan_id>/exportar-pdf/',   views.exportar_pdf_cuotas,   name='exportar_pdf_cuotas'), 
]

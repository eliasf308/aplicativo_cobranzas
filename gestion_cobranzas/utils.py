# gestion_cobranzas/utils.py
import pandas as pd

def cargar_consolidado(periodo: str, hoja: str):
    """
    Lee el archivo Consolidado_ART_MM-AAAA.xlsx y devuelve la lista de grupos.
    Implementación mínima de ejemplo; ajusta a tu lógica real.
    """
    ruta = f"Consolidado_ART_{periodo.replace('/', '-')}.xlsx"
    df = pd.read_excel(ruta, sheet_name=hoja)
    # … agrupar y devolver [{'email':…, 'intimado':…, 'filas': […]}, …]
    raise NotImplementedError("Implementar la lógica de agrupado")

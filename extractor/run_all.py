# extractor/run_all.py
import importlib
import pkgutil
from pathlib import Path
from datetime import datetime

import pandas as pd

from .parser_base import BaseParser
from .common import month_name_es  # si lo tenías en common

# ――― Configuración ―――
PDF_DIR = Path(r"C:\Users\Promecor\Documents\Aplicativo cobranzas\Facturas\2025\Junio")
SALIDA_XLSX = PDF_DIR / "Planes de pago - 06-2025.xlsx"

# ――― Descubrir todos los parsers disponibles ―――
def cargar_parsers() -> list[BaseParser]:
    parser_clases = []

    pkg_path = Path(__file__).with_suffix('').parent / "parsers"
    for mod_info in pkgutil.iter_modules([str(pkg_path)]):
        module = importlib.import_module(f"extractor.parsers.{mod_info.name}")

        # Recorremos los atributos del módulo buscando subclases de BaseParser
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            try:
                if issubclass(attr, BaseParser) and attr is not BaseParser:
                    parser_clases.append(attr)
            except TypeError:
                # attr no es una clase
                pass

    if not parser_clases:
        raise RuntimeError("No se encontró ningún parser.")

    return parser_clases


def main():
    parsers = cargar_parsers()
    filas_totales = []
    duplicados = 0
    nuevos = 0

    # Si ya existe el Excel lo cargamos para evitar duplicar
    if SALIDA_XLSX.exists():
        df_existente = pd.read_excel(SALIDA_XLSX, dtype=str)
    else:
        df_existente = pd.DataFrame()

    for pdf_path in sorted(PDF_DIR.glob("*.pdf")):
        text_sample = pdf_path.read_bytes()[:30_000].decode(
            "latin1", errors="ignore"
        )

        # ―― Elegir el parser adecuado ――
        parser_clase = next(
            (p for p in parsers if p.matches(text_sample)),
            None,
        )
        if not parser_clase:
            print(f"❌ {pdf_path.name}: Aseguradora no reconocida")
            continue

        try:
            filas = parser_clase.extract(pdf_path)
        except Exception as e:
            print(f"❌ {pdf_path.name}: {e}")
            continue

        # ―― Evitar duplicados ――
        for fila in filas:
            clave = (
                fila["Aseguradora"],
                fila["Ramo"],
                fila["Poliza"],
                fila["Endoso"],
                fila["Cuota"],
            )
            if (
                not df_existente.empty
                and ((df_existente[["Aseguradora", "Ramo", "Poliza", "Endoso", "Cuota"]]
                      == clave).all(axis=1)).any()
            ):
                duplicados += 1
            else:
                filas_totales.append(fila)
                nuevos += 1

        print(f"✅ {pdf_path.name}  →  {parser_clase.__name__}")

    # ―― Guardar resultado ――
    if filas_totales:
        df_nuevos = pd.DataFrame(filas_totales)
        final_df = (
            pd.concat([df_existente, df_nuevos], ignore_index=True)
            .sort_values(["Aseguradora", "Poliza", "Cuota"])
        )
        final_df.to_excel(SALIDA_XLSX, index=False)
        print(
            f"\n✅ '{SALIDA_XLSX.name}' actualizado "
            f"con {nuevos} filas nuevas (se omitieron {duplicados} duplicados)."
        )
    else:
        print("\nNo se encontraron filas nuevas (posibles duplicados).")


if __name__ == "__main__":
    inicio = datetime.now()
    main()
    dur = datetime.now() - inicio
    print(f"Tiempo total: {dur.seconds//60} min {dur.seconds%60} s")

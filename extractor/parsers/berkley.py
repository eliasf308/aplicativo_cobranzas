# extractor/parsers/berkley.py
from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import pdfplumber
import pandas as pd

from ..parser_base import BaseParser
from ..common import clean_number, clean_text


class BerkleyParser(BaseParser):
    """Parser para frentes de póliza de Berkley Argentina."""

    # --- Configuración específica ---
    CUIT = "30-50003578-8"           # identificador único en PDFs de Berkley
    KEY_FACTURA = "FACTURA"          # palabra clave para localizar la página
    REG_CAB = re.compile(
        r"SECCION\s+POLIZA\s+SUPLEMENTO", re.I
    )
    REG_VALORES = re.compile(
        r"PA\s*:\s*\$\s*([\d\.,]+).*?PO\s*:\s*\$\s*([\d\.,]+)", re.S
    )
    REG_TABLA = re.compile(
        r"CUOTA\s+FEC\.?VENCTO\.\s+IMPORTE", re.I
    )
    REG_FILA = re.compile(
        r"^\s*(\d{1,3})\s+(\d{2}/\d{2}/\d{4})\s+([\d\.,]+)", re.M
    )
    REG_META = re.compile(
        r"SECCION\s+(.*?)\s+(\d+(?:/\d+)?)\s+(\d+)[\s\-]+(\d+)", re.S
    )
    REG_MONEDA = re.compile(r"\$\s|US\s*\$", re.I)

    @classmethod
    def matches(cls, text_sample: str) -> bool:  # noqa: D401
        """Devuelve True si el PDF pertenece a Berkley."""
        return cls.CUIT in text_sample

    # ---------------------------------------------------------------------

    @classmethod
    def extract(cls, pdf_path: Path) -> list[dict]:
        filas: list[dict] = []

        with pdfplumber.open(pdf_path) as pdf:
            # 1) localizar la/las páginas con FACTURA
            fact_pages = [
                page for page in pdf.pages
                if cls.KEY_FACTURA in page.extract_text()[:30_000]
            ]
            if not fact_pages:
                raise ValueError("Página FACTURA con cuotas no hallada")

            for page in fact_pages:
                text = page.extract_text()

                # --- Aseguradora y moneda ---
                aseguradora = "Berkley"  # fijo; el CUIT ya confirmó

                moneda = "$" if "$" in text else "US$"

                # --- Cabeceras: Ramo, Póliza, Endoso ----------------------
                m_meta = cls.REG_META.search(text)
                if not m_meta:
                    raise ValueError("No se pudo extraer Ramo/Poliza/Endoso")

                ramo_raw, poliza_raw, endoso_raw, _ = m_meta.groups()
                ramo = ramo_raw.strip().upper()
                poliza = clean_number(poliza_raw, int_only=True)
                endoso = clean_number(endoso_raw, int_only=True)

                # --- Filas de la tabla de cuotas --------------------------
                tabla_idx = cls.REG_TABLA.search(text)
                if not tabla_idx:
                    raise ValueError("No se encontraron filas de cuotas")

                for cuota, fecha, importe in cls.REG_FILA.findall(text[tabla_idx.start():]):
                    filas.append(
                        {
                            "Aseguradora": aseguradora,
                            "Ramo": ramo,
                            "Poliza": poliza,
                            "Endoso": endoso,
                            "Moneda": moneda,
                            "Cuota": int(cuota),
                            "Fecha": datetime.strptime(fecha, "%d/%m/%Y").date(),
                            "Importe": clean_number(importe),
                            "Archivo": pdf_path.name,
                        }
                    )

        if not filas:
            raise ValueError("No se generaron filas – revisa el regex de la tabla")

        return filas

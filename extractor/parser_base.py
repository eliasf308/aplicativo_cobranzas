# extractor/parser_base.py
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict

class BaseParser(ABC):
    """
    Interfaz (contrato) que todos los parsers deben cumplir.
    """

    @classmethod
    @abstractmethod
    def matches(cls, pdf_text: str) -> bool:
        """
        Devuelve True si este parser puede procesar el PDF.
        Recibe un texto (por ejemplo, las primeras páginas del PDF).
        """

    @classmethod
    @abstractmethod
    def extract(cls, pdf_path: Path) -> List[Dict]:
        """
        Extrae las filas (cuotas) como lista de diccionarios.
        Cada diccionario tendrá las claves:
        Aseguradora, Ramo, Poliza, Endoso, Moneda, Cuota, Fecha, Importe
        """

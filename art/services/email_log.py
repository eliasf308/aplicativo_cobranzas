# art/services/email_log.py
from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional, Union, Dict, Any, List
from django.contrib.auth import get_user_model
from django.db import transaction

from art.models import EnvioEmailLog, ConsolidadoLote

# Solo para type checking (Pylance/MyPy). No se evalúa en runtime.
if TYPE_CHECKING:
    from django.contrib.auth.models import AbstractBaseUser as DjangoUser

# Runtime: si necesitás el usuario actual u operaciones, seguí usando get_user_model()
User = get_user_model()


def _as_list(value: Optional[Iterable[str]]) -> List[str]:
    if not value:
        return []
    # normalizamos y quitamos vacíos
    return [str(x).strip() for x in value if str(x).strip()]


@transaction.atomic
def log_envio_email(
    *,
    cuit: Union[str, int],
    aseguradora: str = "",
    contrato: Union[str, int, None] = "",
    destinatarios: Optional[Iterable[str]] = None,
    asunto: str,
    cuerpo_resumen: str = "",
    estado: str = "enviado",  # "enviado" | "fallido" | "excluido"
    error: str = "",
    metadata: Optional[Dict[str, Any]] = None,  # ej: {"message_id": "...", "thread_id": "..."}
    usuario: Optional["DjangoUser"] = None,
    lote_consolidado: Optional[ConsolidadoLote] = None,
) -> EnvioEmailLog:
    """
    Crea un registro en EnvioEmailLog. Usar inmediatamente después de intentar enviar el mail.
    Este servicio NO envía emails; solo persiste el resultado.
    """
    dest_list = _as_list(destinatarios)
    data = {
        "usuario": usuario,
        "cuit": str(cuit or "").strip(),
        "aseguradora": aseguradora or "",
        "contrato": str(contrato or "").strip(),
        "destinatarios": dest_list,
        "asunto": (asunto or "")[:255],
        "cuerpo_resumen": cuerpo_resumen or "",
        "estado": estado,
        "error": error or "",
        "metadata": metadata or {},
        "lote_consolidado": lote_consolidado,
    }
    return EnvioEmailLog.objects.create(**data)

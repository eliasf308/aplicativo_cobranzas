# gestion_cobranzas/services/mailer.py
"""
Envía los correos de deuda ART mediante Gmail API (OAuth 2.0) y
devuelve el message_id que genera Gmail.  Se usa desde Celery.

Requisitos:
    pip install --upgrade google-auth google-auth-oauthlib google-api-python-client
"""

import base64
import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, Tuple    # ← añadimos Tuple

from django.template.loader import render_to_string
from django.conf import settings
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from gestion_cobranzas.models import EnvioDeudaART
# ───────────────────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────────────────
_CREDENTIALS_DIR = Path(".gmail_credentials")
_SCOPES = ["https://mail.google.com/"]

# Hoja → alias de credenciales (nombre de <alias>_credentials.json)
HOJA_ALIAS_MAP: Dict[str, str] = {
    "Deuda Promecor": "florencia",
    "Productor": "gimena",
}

# Nombre visible por alias
ALIAS_FROMNAME = {
    "florencia": "Promecor – Cobranzas",
    "gimena": "Promecor – Productores",
}
# Dirección “from”
ALIAS_EMAIL = {
    "florencia": "florencia.meniconi@promecor.com",
    "gimena": "gimena.aldao@promecor.com",
}

# Plantillas (colócalas en gestion_cobranzas/templates/email/)
HTML_TEMPLATE = "email/mail_deuda.html"
TEXT_TEMPLATE = "email/mail_deuda.txt"

# ───────────────────────────────────────────────────────────
# CORE
# ───────────────────────────────────────────────────────────
def _credentials_for(alias: str) -> Credentials:
    """
    Carga / refresca las credenciales OAuth para el alias dado.
    Guarda en disco si se renuevan los tokens.
    """
    base = _CREDENTIALS_DIR
    token_file = base / f"{alias}_token.json"
    cred_file = base / f"{alias}_credentials.json"

    if not token_file.exists():
        raise FileNotFoundError(f"Token OAuth no encontrado: {token_file}")

    creds = Credentials.from_authorized_user_file(token_file, _SCOPES)

    # Refresh token si expiró
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_file.write_text(creds.to_json())

    # Guarda referencia al client_secret si la necesita Google
    creds._client_secret_file = str(cred_file)
    return creds


def _build_service(alias: str):
    creds = _credentials_for(alias)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return service


def _render_body(envio: EnvioDeudaART) -> Tuple[str, str]:
    """
    Renderiza los cuerpos HTML y texto plano usando templates Django.
    Devuelve (html_body, text_body)
    """
    contratos = envio.contratos.all()
    context = {
        "envio": envio,
        "contratos": contratos,
        "fecha_hoy": dt.date.today(),
    }
    html_body = render_to_string(HTML_TEMPLATE, context)
    text_body = render_to_string(TEXT_TEMPLATE, context)
    return html_body, text_body


def _build_message(envio: EnvioDeudaART, alias: str) -> str:
    """
    Construye el mensaje RFC-5322 y lo devuelve codificado en base64url
    (formato requerido por Gmail API).
    """
    html_body, text_body = _render_body(envio)
    from_name = ALIAS_FROMNAME[alias]
    from_email = ALIAS_EMAIL[alias]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = envio.subject
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = envio.email

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    encoded = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    return encoded


# ───────────────────────────────────────────────────────────
# API ÚNICA QUE LLAMA Celery
# ───────────────────────────────────────────────────────────
def enviar_correo_deuda(envio: EnvioDeudaART) -> str:
    """
    Envía el mail y devuelve el message_id. Levanta excepción si falla.
    """
    alias = HOJA_ALIAS_MAP.get(envio.hoja)
    if not alias:
        raise ValueError(f"Hoja desconocida: {envio.hoja}")

    service = _build_service(alias)
    raw_msg = _build_message(envio, alias)

    sent = (
        service.users()
        .messages()
        .send(userId="me", body={"raw": raw_msg})
        .execute()
    )
    return sent.get("id")

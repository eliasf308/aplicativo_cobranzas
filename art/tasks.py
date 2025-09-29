from __future__ import annotations

import os
import logging
from typing import Any, Optional
from pathlib import Path
from base64 import urlsafe_b64encode
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

import pandas as pd  # leer Excel para Productor

from celery import shared_task
from django.conf import settings
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.html import strip_tags  # <-- agregado

from gestion_cobranzas.models import EnvioDeudaART, ContratoEnviado
from art.services.email_log import log_envio_email  # <-- agregado

# Gmail API
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

log = logging.getLogger(__name__)

# ============================== Helpers Excel / Período ==============================

def _periodo_asunto(envio: EnvioDeudaART) -> str:
    """Devuelve el período en formato mm-YYYY (con guion) para el asunto."""
    return envio.fecha_archivo.strftime("%m-%Y") if getattr(envio, "fecha_archivo", None) else ""

def _consolidados_dir() -> Path:
    """
    Directorio donde viven los Consolidado_ART_<mm-YYYY>.xlsx
    Prioriza:
      1) settings.CONSOLIDADOS_DIR
      2) env CONSOLIDADOS_DIR
      3) C:/Users/Promecor/Documents/ART/Deuda ART Historico
    """
    cfg = getattr(settings, "CONSOLIDADOS_DIR", None)
    if cfg:
        return Path(cfg)
    envp = os.getenv("CONSOLIDADOS_DIR", None)
    if envp:
        return Path(envp)
    return Path(r"C:/Users/Promecor/Documents/ART/Deuda ART Historico")

def _find_consolidado_path(periodo_str: str) -> Optional[Path]:
    """Busca el archivo Consolidado del período."""
    base = _consolidados_dir()
    candidates = [
        base / f"Consolidado_ART_{periodo_str}.xlsx",
        base / f"Consolidado {periodo_str}.xlsx",
        base / f"Consolidado_{periodo_str}.xlsx",
    ]
    for p in candidates:
        if p.exists():
            return p
    # Fallback amplio
    try:
        for p in base.glob("*.xlsx"):
            name_low = p.name.lower()
            if "consolidado" in name_low and periodo_str in p.stem:
                return p
    except Exception:
        pass
    return None

def _canon(s: str) -> str:
    """Normaliza nombres de columnas: minúsculas, sin espacios/underscores."""
    return s.lower().replace(" ", "").replace("_", "")

def _find_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Devuelve el nombre real de la primera columna candidata que exista en df (comparación flexible)."""
    cols_map = {_canon(c): c for c in df.columns}
    for cand in candidates:
        key = _canon(cand)
        if key in cols_map:
            return cols_map[key]
    return None

def _productor_from_excel(envio: EnvioDeudaART, contrato_hint: Optional[str] = None) -> Optional[str]:
    """
    Lee el Excel del período (hoja 'Productor') y devuelve el nombre de 'Productor'.
    Intento 1: buscar por CONTRATO (más preciso cuando hay mismo email con varios productores).
    Intento 2: si falla, buscar por EMAIL del envío.
    """
    periodo_str = _periodo_asunto(envio)
    xls_path = _find_consolidado_path(periodo_str)
    if not xls_path:
        log.info("PROD_DEBUG no se encontró archivo Consolidado para %s en %s", periodo_str, _consolidados_dir())
        return None

    try:
        df = pd.read_excel(xls_path, sheet_name="Productor", dtype=str)
    except Exception as e:  # noqa: BLE001
        log.info("PROD_DEBUG error abriendo hoja 'Productor' en %s: %s", xls_path, e)
        return None

    col_email = _find_col(df, ["Email del trato", "Email", "Mail", "Correo", "email del trato", "email_del_trato"])
    col_prod  = _find_col(df, ["Productor"])
    col_cont  = _find_col(df, ["Contrato", "N° de contrato", "Contrato N°", "Nro Contrato"])

    if col_prod is None:
        log.info("PROD_DEBUG no se encontró columna 'Productor' en %s", xls_path.name)
        return None

    # Normalizar
    if col_email:
        df[col_email] = df[col_email].fillna("").str.strip().str.lower()
    if col_cont:
        df[col_cont]  = df[col_cont].fillna("").astype(str).str.strip()
    df[col_prod]  = df[col_prod].fillna("").str.strip()

    # --- Intento 1: por contrato ---
    if contrato_hint and col_cont:
        hint = str(contrato_hint).strip()
        sub = df.loc[df[col_cont] == hint, col_prod].dropna()
        if not sub.empty:
            prod = sub.iloc[0].strip()
            if prod:
                log.info("PROD_DEBUG origen=excel_by_contract archivo=%s contrato=%s productor=%r", xls_path.name, hint, prod)
                return prod

    # --- Intento 2: por email ---
    if col_email:
        email = (envio.email or "").strip().lower()
        sub = df.loc[df[col_email] == email, col_prod].dropna()
        if not sub.empty:
            prod = sub.iloc[0].strip()
            if prod:
                log.info("PROD_DEBUG origen=excel_by_email archivo=%s productor=%r", xls_path.name, prod)
                return prod

    log.info("PROD_DEBUG sin coincidencia (contrato=%r email=%r) en %s", contrato_hint, getattr(envio, "email", None), xls_path.name)
    return None

# ============================== Templates ==============================

def _render_mail_html(
    envio: EnvioDeudaART,
    filas: list[dict[str, Any]],
    body_variant: str,
    razon_saludo: str | None,
) -> str:
    ctx = {
        "envio": envio,
        "filas": filas,
        "periodo": getattr(envio, "fecha_archivo", None),
        "body_variant": body_variant,   # "menor3" | "mayorigual3"
        "razon_saludo": razon_saludo,
    }
    log.info("Renderizando template EMAIL: art_app/art/mail_deuda.html")
    return render_to_string("art_app/art/mail_deuda.html", ctx)

def _render_mail_text(
    envio: EnvioDeudaART,
    filas: list[dict[str, Any]],
    body_variant: str,
    razon_saludo: str | None,
) -> str:
    periodo = envio.fecha_archivo.strftime("%m/%Y") if getattr(envio, "fecha_archivo", None) else ""
    saludo = f"Estimado/a {razon_saludo}\n\n" if razon_saludo else "Estimado/a,\n\n"

    if body_variant == "menor3":
        cuerpo = (
            "Nos ponemos en contacto desde Promecor, su Broker de Seguros, con el objetivo de informarle que a la fecha "
            "tiene un saldo pendiente con su actual ART.\n\n"
            "Consideramos oportuno dar aviso de la situación para que, en la medida de lo posible, podamos accionar en "
            "consecuencia y verificar si esto corresponde a conceptos no remunerativos o si hace falta gestionar un pago cancelatorio.\n\n"
            f"Al {periodo}, el saldo pendiente es el que se detalla a continuación.\n\n"
        )
    else:
        cuerpo = (
            "Nos ponemos en contacto desde Promecor, su Broker de Seguros, para informarle que a la fecha tiene un saldo "
            "pendiente con su actual ART, por lo cual el contrato se encuentra INTIMADO en proceso de anulación.\n\n"
            "De acuerdo con la legislación vigente (art. 27 ley 24.557), se inicia el proceso de intimación y anulación de la "
            "cobertura por falta de pago a partir de la segunda cuota adeudada.\n\n"
            "El saldo adeudado informado por la Compañía es el siguiente:\n\n"
        )

    filas_txt = "\n".join(
        f"- Contrato {f['contrato']} · {f['razon_social']} · $ {Decimal(f['deuda_total'] or 0):,.2f}"
        .replace(",", "X").replace(".", ",").replace("X", ".")
        for f in filas
    )

    tail = (
        "\n\nSi corresponde, deberá abonar el importe generando un VEP de pago a través de la página de AFIP y transferirlo por su entidad bancaria.\n"
        "Una vez realizada esta gestión, o si ya está abonada la deuda, por favor envíe el VEP y su comprobante de pago para actualizar el saldo.\n\n"
        "VEP Capital: Impuesto 312 – Concepto 19 – Subconcepto 19 – Período Fiscal (mes anterior al actual / año actual)\n"
        "VEP Intereses: Impuesto 312 – Concepto 19 – Subconcepto 51 – Período Fiscal (mes anterior al actual / año actual)\n\n"
        "Si necesita un estado de cuenta, solicítelo por este medio. Quedamos a disposición.\n\n"
        "Cobranzas Promecor"
    )
    return f"DEUDA ART - {periodo}\n\n{saludo}{cuerpo}{filas_txt}{tail}"

# ============================== Gmail API ==============================

def _get_sender_alias(envio: EnvioDeudaART) -> str:
    alias = getattr(envio, "desde_cuenta", None) \
        or os.getenv("GMAIL_SENDER_ALIAS") \
        or getattr(settings, "GMAIL_SENDER_ALIAS", None)
    return (alias or "florencia").strip().lower()

def _gmail_service_for(alias: str):
    base_dir = Path(settings.BASE_DIR)
    token_file = base_dir / ".gmail_credentials" / f"{alias}_token.json"
    if not token_file.exists():
        raise FileNotFoundError(f"Token OAuth no encontrado para alias '{alias}': {token_file}")

    scopes = ["https://mail.google.com/"]
    creds = Credentials.from_authorized_user_file(str(token_file), scopes=scopes)

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise RuntimeError(
                f"Credenciales inválidas para alias '{alias}'. Reautorizar con authorize_gmail.py"
            )

    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def _load_logo_bytes() -> bytes | None:
    default_path = Path(r"C:/Users/Promecor/Documents/logo.png")
    cfg = getattr(settings, "GMAIL_LOGO_PATH", None)
    envp = os.getenv("LOGO_PATH", None)
    path = Path(cfg) if cfg else (Path(envp) if envp else default_path)
    try:
        return path.read_bytes()
    except Exception:
        log.warning("No se pudo cargar el logo en %s (se envía sin logo).", path)
        return None

def _gmail_send_related_html(service, to_email: str, subject: str, html: str, text: str, logo_bytes: bytes | None) -> str:
    outer = MIMEMultipart("related")
    outer["To"] = to_email
    outer["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text, "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))
    outer.attach(alt)

    if logo_bytes:
        img = MIMEImage(logo_bytes, "png")
        img.add_header("Content-ID", "<promecor-logo>")
        img.add_header("Content-Disposition", "inline", filename="logo.png")
        outer.attach(img)

    raw = urlsafe_b64encode(outer.as_bytes()).decode("utf-8")
    resp = service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return resp.get("id", "")

# ============================== Asunto ==============================

def _get_productor(envio: EnvioDeudaART, filas: list[dict[str, Any]]) -> Optional[str]:
    """
    1) Si en las filas hay una clave 'productor', usarla.
    2) Si no, leer del Excel por CONTRATO (más preciso). Si falla, por EMAIL.
    """
    if filas and isinstance(filas[0], dict):
        for k, v in filas[0].items():
            if v and isinstance(k, str) and "productor" in k.lower():
                val = str(v).strip()
                if val:
                    log.info("PROD_DEBUG origen=filas key=%s value=%r", k, val)
                    return val

    contrato_hint = None
    if filas and isinstance(filas[0], dict):
        contrato_hint = str(filas[0].get("contrato") or "").strip()

    return _productor_from_excel(envio, contrato_hint=contrato_hint)

def _build_subject(envio: EnvioDeudaART, filas: list[dict[str, Any]]) -> str:
    """
    Reglas:
    - Hoja 'Productor':
        - si agrupa >=3 contratos -> "DEUDA ART - <mm-YYYY>"
        - si no -> "DEUDA ART - <Productor> <mm-YYYY>"
    - Deuda Promecor (u otras hojas):
        - si agrupa >=3 contratos -> "DEUDA ART - <mm-YYYY>"
        - si 1 o 2 -> "DEUDA ART - <Razón social> <CUIT> <Aseguradora> <mm-YYYY>"
    """
    periodo_str = _periodo_asunto(envio)
    hoja = (getattr(envio, "hoja", "") or "").strip().lower()
    n = len(filas)

    productor = _get_productor(envio, filas) if "productor" in hoja else None
    keys_first = list(filas[0].keys()) if filas and isinstance(filas[0], dict) else []
    log.info("ASUNTO_DEBUG hoja=%s n=%d periodo=%s productor=%r keys=%s", hoja, n, periodo_str, productor, keys_first)

    if "productor" in hoja:
        if n >= 3:
            subject = f"DEUDA ART - {periodo_str}".strip()
        else:
            subject = f"DEUDA ART - {productor} {periodo_str}".strip() if productor else f"DEUDA ART - {periodo_str}".strip()
    else:
        if n >= 3:
            subject = f"DEUDA ART - {periodo_str}".strip()
        elif filas:
            f = filas[0]
            razon = (f.get("razon_social") or "").strip()
            cuit = (str(f.get("cuit") or "")).strip()
            aseg = (f.get("aseguradora") or "").strip()
            parts = [p for p in (razon, cuit, aseg, periodo_str) if p]
            subject = "DEUDA ART - " + " ".join(parts)
        else:
            subject = f"DEUDA ART - {periodo_str}".strip()

    log.info("ASUNTO_DEBUG subject_final=%s", subject)
    return subject

# ============================== Tarea Celery ==============================

@shared_task
def task_enviar_mails(envios_ids: list[int] | int) -> dict:
    """
    Envía 1 mail por cada EnvioDeudaART (Gmail API, alias por token OAuth).
    Acepta un entero (single) o una lista de IDs.
    Actualiza: estado (ENVIADO/ERROR), message_id y detalle_error.
    """
    ids = [envios_ids] if isinstance(envios_ids, int) else list(envios_ids)
    resumen = {"procesados": 0, "ok": 0, "error": 0, "ids": ids}

    for envio_id in ids:
        envio = EnvioDeudaART.objects.get(pk=envio_id)

        # Recuperamos las filas ANTES del envío para loguear también si falla
        filas = list(
            ContratoEnviado.objects.filter(envio=envio)
            .order_by("razon_social")
            .values(
                "contrato",
                "razon_social",
                "cuit",
                "aseguradora",
                "deuda_total",
                "q_periodos",
                "intimado",
            )
        )

        html = ""      # para que exista en except si falla antes del render
        subject = ""   # idem

        try:
            to = (envio.email or "").strip()
            if not to:
                raise ValueError("El envío no tiene destinatario (email vacío).")

            # --------- cuerpo: REGLA FINAL ----------
            count = len(filas)

            def _to_int(v):
                try:
                    return int(v or 0)
                except Exception:
                    return 0

            q_vals = [_to_int(f.get("q_periodos")) for f in filas]
            if count >= 3:
                # En grupos de 3+ SIEMPRE cuerpo Suave (badges por fila si corresponde)
                body_variant = "menor3"
            else:
                # 1 contrato: INTIMADO solo si ese único q>=3
                q0 = q_vals[0] if q_vals else 0
                body_variant = "mayorigual3" if q0 >= 3 else "menor3"
            # ----------------------------------------

            # Saludo (si hay >=3 contratos, saludo genérico)
            if count >= 3:
                razon_saludo = None
            else:
                razon_saludo = str(filas[0].get("razon_social") or "").strip() if filas else None
                if not razon_saludo:
                    razon_saludo = None

            # Asunto
            subject = _build_subject(envio, filas)

            # Cuerpos
            html = _render_mail_html(envio, filas, body_variant, razon_saludo)
            text = _render_mail_text(envio, filas, body_variant, razon_saludo)

            # Envío Gmail
            alias = _get_sender_alias(envio)
            service = _gmail_service_for(alias)
            logo_bytes = _load_logo_bytes()
            message_id = _gmail_send_related_html(service, to, subject, html, text, logo_bytes)

            # Tracking del EnvioDeudaART
            envio.subject = subject
            envio.fecha_envio = envio.fecha_envio or timezone.now()
            envio.message_id = message_id or ""
            envio.estado = "ENVIADO"
            envio.detalle_error = ""
            envio.save(update_fields=["subject", "fecha_envio", "message_id", "estado", "detalle_error"])

            # ===== LOG POR CUIT (éxito) =====
            resumen_txt = strip_tags(html)[:2000]
            usuario_env = getattr(envio, "enviado_por", None)
            for f in filas:
                log_envio_email(
                    cuit=f.get("cuit", ""),
                    aseguradora=str(f.get("aseguradora", "")),
                    contrato=str(f.get("contrato", "")),
                    destinatarios=[to],
                    asunto=subject,
                    cuerpo_resumen=resumen_txt,
                    estado="enviado",
                    error="",
                    metadata={"message_id": message_id} if message_id else {},
                    usuario=usuario_env,
                    lote_consolidado=None,
                )

            resumen["ok"] += 1

        except Exception as exc:  # noqa: BLE001
            detalle = str(exc)
            log.exception("Error enviando EnvioDeudaART id=%s: %s", envio_id, detalle)

            envio.estado = "ERROR"
            envio.detalle_error = (detalle[:950] + "...") if len(detalle) > 950 else detalle
            envio.save(update_fields=["estado", "detalle_error"])

            # ===== LOG POR CUIT (fallido) =====
            resumen_txt = strip_tags(html)[:2000] if html else ""
            subject_fallback = subject or f"DEUDA ART - {_periodo_asunto(envio)}"
            usuario_env = getattr(envio, "enviado_por", None)
            to = (envio.email or "").strip()
            for f in filas:
                log_envio_email(
                    cuit=f.get("cuit", ""),
                    aseguradora=str(f.get("aseguradora", "")),
                    contrato=str(f.get("contrato", "")),
                    destinatarios=[to] if to else [],
                    asunto=subject_fallback,
                    cuerpo_resumen=resumen_txt,
                    estado="fallido",
                    error=detalle,
                    metadata={},
                    usuario=usuario_env,
                    lote_consolidado=None,
                )

            resumen["error"] += 1

        finally:
            resumen["procesados"] += 1

    return resumen


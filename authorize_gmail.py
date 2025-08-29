# authorize_gmail.py
"""
Ejemplos de uso:
    python authorize_gmail.py florencia
    python authorize_gmail.py productores

Requisitos:
    - En .gmail_credentials debe existir el archivo de credenciales OAuth
      con el nombre <alias>_credentials.json
      (ej.: florencia_credentials.json, productores_credentials.json)

Salida:
    - Genera .gmail_credentials/<alias>_token.json
"""

from __future__ import annotations

import sys
from pathlib import Path
from google_auth_oauthlib.flow import InstalledAppFlow

# Scope alineado con tasks.py (permite enviar/leer/gestionar mails)
SCOPES = ["https://mail.google.com/"]


def main(alias: str) -> None:
    base = Path(".gmail_credentials")
    base.mkdir(parents=True, exist_ok=True)

    cred_file = base / f"{alias}_credentials.json"
    token_file = base / f"{alias}_token.json"

    if not cred_file.exists():
        print(f"❌ No se encontró el archivo de credenciales: {cred_file}")
        print("   Colocá el client secret de esa cuenta con ese nombre y reintentá.")
        sys.exit(1)

    # Inicia el flujo OAuth en un navegador local
    flow = InstalledAppFlow.from_client_secrets_file(str(cred_file), scopes=SCOPES)
    creds = flow.run_local_server(port=0)  # abre navegador para consentir

    # Guarda el token para que lo use tasks.py
    token_file.write_text(creds.to_json(), encoding="utf-8")
    print(f"✅ Token guardado en {token_file}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python authorize_gmail.py <alias>   (p.ej. florencia | productores)")
        sys.exit(1)
    main(sys.argv[1].strip().lower())

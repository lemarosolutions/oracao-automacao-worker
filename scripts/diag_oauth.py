# scripts/diag_oauth.py
# Diagnóstico de OAuth/Drive sem definir scopes no código.
# Usa os escopos já embutidos no refresh_token.
import os
import sys
import json
import datetime
import requests

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------- Config ----------
TOKEN_URI = "https://oauth2.googleapis.com/token"
TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo"

def fail(msg: str, code: int = 1):
    print(f"DIAG_FAIL: {msg}")
    sys.exit(code)

def creds_from_env() -> Credentials:
    """Monta credenciais SEM scopes (deixa o refresh_token decidir)."""
    try:
        return Credentials(
            token=None,
            refresh_token=os.environ["OAUTH_REFRESH_TOKEN"],
            client_id=os.environ["OAUTH_CLIENT_ID"],
            client_secret=os.environ["OAUTH_CLIENT_SECRET"],
            token_uri=TOKEN_URI,
        )
    except KeyError as e:
        fail(f"Variável de ambiente ausente: {e}")

def tokeninfo(access_token: str) -> dict:
    try:
        r = requests.get(TOKENINFO_URL, params={"access_token": access_token}, timeout=20)
        if r.status_code == 200:
            return r.json()
        return {"error": f"tokeninfo_http_{r.status_code}", "body": r.text[:500]}
    except Exception as e:
        return {"error": str(e)}

def ensure_folder_exists(drive_svc, parent_id: str, name: str) -> str:
    q = (
        f"'{parent_id}' in parents and "
        f"name = '{name}' and "
        "mimeType = 'application/vnd.google-apps.folder' and "
        "trashed = false"
    )
    resp = drive_svc.files().list(q=q, fields="files(id,name)").execute()
    items = resp.get("files", [])
    if items:
        return items[0]["id"]
    # não cria — apenas diagnostica
    return ""

def main():
    started = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    root_id = os.environ.get("DRIVE_ROOT_FOLDER_ID", "").strip()
    sheet_id = os.environ.get("SHEET_ID", "").strip()

    if not root_id:
        fail("DRIVE_ROOT_FOLDER_ID não definido nos secrets.")

    # 1) Refresh sem scopes no código
    creds = creds_from_env()
    try:
        creds.refresh(Request())
    except Exception as e:
        fail(f"RefreshError: {e}")

    # 2) Inspeciona escopos reais do access_token
    ti = tokeninfo(creds.token or "")
    scopes = ti.get("scope", "")
    # pode vir string com escopos separados por espaço ou vazio
    scopes_list = scopes.split() if isinstance(scopes, str) else []

    # 3) Conecta no Drive
    try:
        drive = build("drive", "v3", credentials=creds)
    except Exception as e:
        fail(f"Falha ao buildar serviço Drive: {e}")

    # 4) Verifica pasta raiz e 00_config
    try:
        about = drive.about().get(fields="user(emailAddress,displayName)").execute()
        user_email = about.get("user", {}).get("emailAddress", "unknown")

        # valida acesso ao root
        _ = drive.files().get(fileId=root_id, fields="id,name").execute()

        cfg_id = ensure_folder_exists(drive, root_id, "00_config")

    except HttpError as he:
        fail(f"Drive HttpError: {he}")
    except Exception as e:
        fail(f"Erro ao consultar Drive: {e}")

    out = {
        "utc": started,
        "status": "OK",
        "who": user_email,
        "root_id": root_id,
        "config_id": cfg_id or "(00_config não encontrada – só diagnóstico, não cria)",
        "sheet_id": sheet_id or "(não definido)",
        "scopes_from_tokeninfo": scopes_list,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
    sys.exit(0)

if __name__ == "__main__":
    main()

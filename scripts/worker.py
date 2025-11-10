# scripts/worker.py
# -*- coding: utf-8 -*-
"""
Worker mínimo para validar OAuth e acesso ao Drive/Sheets.
- Lê variáveis de ambiente do GitHub Actions.
- Constrói serviços usando OAuth (client id/secret + refresh token).
- Garante pastas-base e grava um log de batimento.
"""

import os
import io
import json
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# ===== Scopes EXATOS (não altere) =====
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

# ===== Helpers de Drive =====
def find_child_by_name(drive, parent_id, name):
    q = f"'{parent_id}' in parents and trashed=false and name='{name}'"
    r = drive.files().list(q=q, fields="files(id,name,mimeType)").execute()
    files = r.get("files", [])
    return files[0] if files else None

def ensure_folder(drive, parent_id, name):
    f = find_child_by_name(drive, parent_id, name)
    if f and f.get("mimeType") == "application/vnd.google-apps.folder":
        return f["id"]
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    f = drive.files().create(body=meta, fields="id,name").execute()
    return f["id"]

def upload_text(drive, parent_id, filename, content):
    # sobrescreve se já existir
    existing = find_child_by_name(drive, parent_id, filename)
    if existing:
        drive.files().delete(fileId=existing["id"]).execute()
    media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/plain")
    meta = {"name": filename, "parents": [parent_id]}
    return drive.files().create(body=meta, media_body=media, fields="id,name").execute()

# ===== Autenticação OAuth (com refresh token) =====
def build_services_from_oauth():
    client_id = os.getenv("OAUTH_CLIENT_ID", "").strip()
    client_secret = os.getenv("OAUTH_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("OAUTH_REFRESH_TOKEN", "").strip()

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError("Falta OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET ou OAUTH_REFRESH_TOKEN nos Secrets.")

    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )
    # força refresh para validar escopos/credenciais
    creds.refresh(Request())

    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

# ===== Main =====
def main():
    root_id = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    if not root_id:
        raise RuntimeError("Falta DRIVE_ROOT_FOLDER_ID nos Secrets.")

    drive, _ = build_services_from_oauth()

    # Garante estrutura mínima
    cfg_id  = ensure_folder(drive, root_id, "00_config")
    scp_id  = ensure_folder(drive, root_id, "02_scripts_autogerados")
    logs_id = ensure_folder(drive, root_id, "05_logs")

    # Loga batimento/validação
    now = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    lines = [
        f"UTC: {now}",
        "status: OK",
        "step: oauth_validated_and_folders_checked",
        f"root: {root_id}",
        f"00_config: {cfg_id}",
        f"02_scripts_autogerados: {scp_id}",
        f"05_logs: {logs_id}",
        "scopes: " + ", ".join(SCOPES),
    ]
    upload_text(drive, logs_id, f"log_worker_{now}.txt", "\n".join(lines))

if __name__ == "__main__":
    try:
        main()
        print("Worker finalizado com sucesso.")
    except HttpError as e:
        print(f"HttpError: {e}")
        raise
    except Exception as e:
        print(f"Erro: {e}")
        raise

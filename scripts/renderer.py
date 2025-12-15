# scripts/renderer.py
# SAFE_MODE – BLINDADO contra formatos inválidos de work_orders

import os, io, json, tempfile, shutil, subprocess as sp
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

# ================= CONFIG =================
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]

# ================= UTILS =================
def sh(cmd):
    cp = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
    if cp.returncode != 0:
        raise RuntimeError(cp.stdout)
    return cp.stdout

def to_str(v):
    if v is None:
        return ""
    return str(v).strip()

# ================= AUTH =================
def drive_service():
    sa = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
    if sa:
        info = json.loads(sa)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("drive", "v3", credentials=creds)

    creds = Credentials(
        None,
        refresh_token=os.getenv("OAUTH_REFRESH_TOKEN"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.getenv("OAUTH_CLIENT_ID"),
        client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
        scopes=SCOPES
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

# ================= DRIVE =================
def download_text(svc, file_id):
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue().decode("utf-8")

def ensure_folder(svc, parent, name):
    q = f"'{parent}' in parents and trashed=false and name='{name}'"
    r = svc.files().list(q=q, fields="files(id)").execute().get("files", [])
    if r:
        return r[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]}
    return svc.files().create(body=meta, fields="id").execute()["id"]

# ================= NORMALIZAÇÃO =================
def normalize_jobs(raw):
    """
    Aceita:
    - lista de dicts
    - dict com chave 'orders'
    - string JSON
    - string única
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except:
            return []

    if isinstance(raw, dict):
        raw = raw.get("orders", [])

    if isinstance(raw, list):
        return [j for j in raw if isinstance(j, dict)]

    return []

# ================= MAIN =================
def run():
    svc = drive_service()
    ROOT = os.getenv("DRIVE_ROOT_FOLDER_ID")

    cfg = ensure_folder(svc, ROOT, "00_config")

    q = f"'{cfg}' in parents and name contains 'work_orders_'"
    r = svc.files().list(q=q, orderBy="modifiedTime desc", pageSize=1).execute()

    if not r.get("files"):
        raise RuntimeError("Nenhum work_orders encontrado.")

    raw_text = download_text(svc, r["files"][0]["id"])
    raw_json = json.loads(raw_text)

    jobs = normalize_jobs(raw_json)

    if not jobs:
        raise RuntimeError("work_orders sem jobs válidos após normalização.")

    for job in jobs:
        slot = to_str(job.get("slot"))
        lang = to_str(job.get("idioma") or "pt")
        faixa = to_str(job.get("faixa_ave_maria"))

        print("JOB OK:", slot, lang, faixa)

if __name__ == "__main__":
    run()

# scripts/renderer.py
# SAFE_MODE renderer – versão BLINDADA contra tipos inválidos (string/float/None)

import os, io, json, random, tempfile, shutil, re, math
from datetime import datetime
import subprocess as sp

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

from PIL import Image, ImageDraw, ImageFont
from gtts import gTTS

# ================= CONFIG =================
TARGET_SEC = 480
FPS = 30
W, H = 1920, 1080
MIN_SLIDESHOW_SEC = 60.0

IMG_EXTS = ('.jpg', '.jpeg', '.png')
AUD_EXTS = ('.mp3', '.wav', '.m4a', '.aac')

SAY_TYPES = {
    'abertura','exame','suplica','súplica','verso','salmo',
    'meditacao','meditação','intercessao','intercessão',
    'agradecimento','encerramento','cta','texto','mensagem'
}

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

def to_str(val):
    if val is None:
        return ""
    return str(val).strip()

def ffprobe_duration(path):
    out = sh(f'ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "{path}"')
    return float(out.strip())

# ================= AUTH =================
def build_drive_service():
    sa_json = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("drive", "v3", credentials=creds)

    client_id = os.getenv("OAUTH_CLIENT_ID")
    client_secret = os.getenv("OAUTH_CLIENT_SECRET")
    refresh_token = os.getenv("OAUTH_REFRESH_TOKEN")

    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds)

# ================= DRIVE =================
def list_by_name(svc, parent_id, name):
    q = f"'{parent_id}' in parents and trashed=false and name='{name}'"
    r = svc.files().list(q=q, fields="files(id,name)").execute()
    return r.get("files", [])

def ensure_folder(svc, parent_id, name):
    r = list_by_name(svc, parent_id, name)
    if r:
        return r[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    return svc.files().create(body=meta, fields="id").execute()["id"]

def download_text(svc, file_id):
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue().decode("utf-8")

def upload_file(svc, parent_id, local_path, name, mime):
    media = MediaIoBaseUpload(open(local_path, "rb"), mimetype=mime, resumable=True)
    meta = {"name": name, "parents": [parent_id]}
    return svc.files().create(body=meta, media_body=media, fields="id").execute()["id"]

def list_files(svc, folder_id):
    q = f"'{folder_id}' in parents and trashed=false"
    r = svc.files().list(q=q, fields="files(id,name)").execute()
    return r.get("files", [])

# ================= TSV =================
def load_tsv_rows(path):
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for ln in f:
            p = ln.rstrip("\n").split("\t")
            if len(p) >= 3:
                try:
                    rows.append({
                        "ord": int(p[0]),
                        "tipo": to_str(p[1]).lower(),
                        "txt": to_str(p[2])
                    })
                except:
                    continue
    rows.sort(key=lambda x: x["ord"])
    return rows

def narration_from_rows(rows):
    texts = []
    policy = "bg_random"
    faixa_ave = ""

    for r in rows:
        if r["tipo"] == "musica_policy":
            policy = to_str(r["txt"]).lower()
        elif r["tipo"] == "faixa_ave_maria":
            faixa_ave = to_str(r["txt"])
        elif r["tipo"] in SAY_TYPES:
            texts.append(to_str(r["txt"]))

    base = " ".join(texts)
    if len(base.split()) < 700:
        base = (base + " ") * 3

    return base.strip(), policy, faixa_ave

# ================= VIDEO =================
def build_slideshow(imgs, dur, out_mp4):
    per = max(3.0, dur / len(imgs))
    inputs = " ".join([f'-loop 1 -t {per:.2f} -i "{p}"' for p in imgs])
    chain = ";".join([f"[{i}:v]scale={W}:{H},fps={FPS}[v{i}]" for i in range(len(imgs))])
    concat = "".join([f"[v{i}]" for i in range(len(imgs))])
    sh(
        f'ffmpeg -y {inputs} -filter_complex "{chain};{concat}concat=n={len(imgs)}:v=1:a=0[out]" '
        f'-map "[out]" -t {dur:.2f} "{out_mp4}"'
    )

# ================= MAIN =================
def run():
    svc = build_drive_service()
    ROOT = os.getenv("DRIVE_ROOT_FOLDER_ID")
    cfg = ensure_folder(svc, ROOT, "00_config")
    scripts = ensure_folder(svc, ROOT, "02_scripts_autogerados")

    q = f"'{cfg}' in parents and name contains 'work_orders_'"
    r = svc.files().list(q=q, orderBy="modifiedTime desc", pageSize=1).execute()
    jobs = json.loads(download_text(svc, r["files"][0]["id"]))

    tmp = tempfile.mkdtemp()
    try:
        for job in jobs:
            slot = to_str(job.get("slot"))
            lang = to_str(job.get("idioma") or "pt")
            faixa_job = to_str(job.get("faixa_ave_maria"))

            tsv_name = f"run_{slot}.tsv"
            tsv_file = list_by_name(svc, scripts, tsv_name)
            if not tsv_file:
                continue

            tsv_path = os.path.join(tmp, tsv_name)
            req = svc.files().get_media(fileId=tsv_file[0]["id"])
            with open(tsv_path, "wb") as f:
                dl = MediaIoBaseDownload(f, req)
                done = False
                while not done:
                    _, done = dl.next_chunk()

            rows = load_tsv_rows(tsv_path)
            text, policy, faixa_tsv = narration_from_rows(rows)

            # Apenas validação por enquanto
            print("OK:", slot, lang, policy, faixa_job or faixa_tsv)

    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    run()

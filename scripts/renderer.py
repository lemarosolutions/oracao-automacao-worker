import os, io, random, json, tempfile, re, math, shutil, subprocess as sp
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from gtts import gTTS
from PIL import Image, ImageDraw, ImageFont

# ---------------- CONFIG ----------------
TARGET_SEC = 480
FPS = 30
W, H = 1920, 1080

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]

# ---------------- SHELL ----------------
def sh(cmd):
    p = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stdout)
    return p.stdout

# ---------------- OAUTH ----------------
def drive_service():
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

# ---------------- DRIVE ----------------
def list_by_name(svc, parent, name):
    q = f"'{parent}' in parents and trashed=false and name='{name}'"
    r = svc.files().list(q=q, fields="files(id,name)").execute()
    return r.get("files", [])

def ensure_folder(svc, parent, name):
    r = list_by_name(svc, parent, name)
    if r:
        return r[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent]}
    return svc.files().create(body=meta, fields="id").execute()["id"]

def download_any(svc, folder, exts, limit=20):
    q = f"'{folder}' in parents and trashed=false"
    r = svc.files().list(q=q, fields="files(id,name)").execute()
    files = [f for f in r.get("files", []) if f["name"].lower().endswith(exts)]
    random.shuffle(files)
    paths = []
    for f in files[:limit]:
        req = svc.files().get_media(fileId=f["id"])
        fd, tmp = tempfile.mkstemp(suffix="_"+f["name"])
        os.close(fd)
        with open(tmp, "wb") as out:
            dl = MediaIoBaseDownload(out, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
        paths.append(tmp)
    return paths

# ---------------- MEDIA ----------------
def tts(text, out):
    mp3 = out.replace(".wav", ".mp3")
    gTTS(text=text, lang="pt").save(mp3)
    sh(f'ffmpeg -y -i "{mp3}" -ar 44100 "{out}"')
    os.remove(mp3)

def slideshow_motion(imgs, dur, out):
    per = max(4.0, dur / len(imgs))
    txt = tempfile.mktemp(suffix=".txt")

    with open(txt, "w") as f:
        for img in imgs:
            f.write(f"file '{img}'\n")
            f.write(f"duration {per}\n")

    sh(
        f'ffmpeg -y -f concat -safe 0 -i "{txt}" '
        f'-vf "scale=iw*1.1:ih*1.1,crop={W}:{H}" '
        f'-r {FPS} -t {dur} "{out}"'
    )

def seconds(path):
    return float(sh(f'ffprobe -v error -show_entries format=duration -of csv=p=0 "{path}"').strip())

# ---------------- MAIN ----------------
def run():
    svc = drive_service()
    ROOT = os.getenv("DRIVE_ROOT_FOLDER_ID")

    img_j = ensure_folder(svc, ROOT, "01_assets_imagens_jesus")
    img_m = ensure_folder(svc, ROOT, "01_assets_imagens_maria")
    out_pt = ensure_folder(svc, ROOT, "03_outputs_videos_pt")

    imgs = download_any(svc, img_j, (".jpg", ".png"), 20)
    if not imgs:
        raise RuntimeError("Sem imagens")

    tmp = tempfile.mkdtemp()
    try:
        narr = os.path.join(tmp, "narr.wav")
        tts("Senhor, entregamos este momento a Ti.", narr)
        dur = min(seconds(narr), TARGET_SEC)

        vid = os.path.join(tmp, "video.mp4")
        slideshow_motion(imgs, dur, vid)

        sh(
            f'ffmpeg -y -stream_loop -1 -i "{vid}" -i "{narr}" '
            f'-shortest -t {TARGET_SEC} '
            f'-c:v libx264 -pix_fmt yuv420p "{tmp}/final.mp4"'
        )

        MediaIoBaseUpload(open(f"{tmp}/final.mp4", "rb"), mimetype="video/mp4")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    run()

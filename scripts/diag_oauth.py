import os, sys, io, json, datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
import requests

REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/drive",                 # leitura + escrita
    "https://www.googleapis.com/auth/spreadsheets.readonly", # leitura sheets
]

OAUTH_CLIENT_ID     = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_REFRESH_TOKEN = os.getenv("OAUTH_REFRESH_TOKEN")
DRIVE_ROOT_ID       = os.getenv("DRIVE_ROOT_FOLDER_ID")
SHEET_ID            = os.getenv("SHEET_ID")

def die(msg, code=1):
    print("DIAG_FAIL:", msg)
    sys.exit(code)

def tokeninfo(access_token):
    try:
        r = requests.get("https://oauth2.googleapis.com/tokeninfo", params={"access_token": access_token}, timeout=20)
        if r.status_code == 200:
            return r.json()
        return {"error": f"tokeninfo_http_{r.status_code}", "body": r.text[:200]}
    except Exception as e:
        return {"error": str(e)}

def main():
    # 0) Secrets básicos
    for k,v in {"OAUTH_CLIENT_ID":OAUTH_CLIENT_ID, "OAUTH_CLIENT_SECRET":OAUTH_CLIENT_SECRET, "OAUTH_REFRESH_TOKEN":OAUTH_REFRESH_TOKEN, "DRIVE_ROOT_FOLDER_ID":DRIVE_ROOT_ID}.items():
        if not v:
            die(f"Secret ausente: {k}")

    # 1) Monta credenciais com escopos finais
    creds = Credentials(
        None,
        refresh_token=OAUTH_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=OAUTH_CLIENT_ID,
        client_secret=OAUTH_CLIENT_SECRET,
        scopes=REQUIRED_SCOPES,
    )

    # 2) Refresh para obter access_token com os escopos concedidos
    creds.refresh(Request())
    at = creds.token
    if not at:
        die("Access token não obtido do refresh")

    # 3) tokeninfo: conferir e-mail e escopos concedidos
    info = tokeninfo(at)
    print("tokeninfo:", json.dumps(info, ensure_ascii=False))
    if "email" in info:
        email = info["email"]
    else:
        # fallback: Drive About
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        me = drive.about().get(fields="user(emailAddress,displayName)").execute()
        email = me["user"]["emailAddress"]
        print("drive.about.user:", me["user"])

    granted_scopes = set(info.get("scope", "").split()) if "scope" in info else set()
    # Se tokeninfo não trouxe scope, vamos assumir os REQUIRED_SCOPES e validar na prática (operação abaixo)
    print("granted_scopes:", " ".join(sorted(granted_scopes)) if granted_scopes else "(não reportado por tokeninfo)")

    # 4) Checagem prática: listagem em 00_config e escrita em 05_logs
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    # 4.1) Descobrir subpastas críticas
    q = f"'{DRIVE_ROOT_ID}' in parents and trashed=false"
    children = drive.files().list(q=q, fields="files(id,name)").execute().get("files", [])
    names = {c["name"]: c["id"] for c in children}
    for required in ["00_config", "05_logs"]:
        if required not in names:
            die(f"Pasta obrigatória ausente em ROOT: {required}")
    cfg_id = names["00_config"]
    logs_id = names["05_logs"]

    # 4.2) Teste de leitura em 00_config
    _ = drive.files().list(q=f"'{cfg_id}' in parents and trashed=false", pageSize=1, fields="files(id,name)").execute()

    # 4.3) Teste de escrita em 05_logs (requer escopo drive FULL)
    now = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    content = f"diag_ok at {now} UTC | account={email}"
    media = MediaIoBaseUpload(io.BytesIO(content.encode("utf-8")), mimetype="text/plain")
    meta = {"name": f"diag_{now}.txt", "parents": [logs_id]}
    drive.files().create(body=meta, media_body=media, fields="id,name").execute()

    print("DIAG_OK: email=", email)
    print("DIAG_OK: write test succeeded; renderer pode escrever nos outputs.")
    sys.exit(0)

if __name__ == "__main__":
    main()

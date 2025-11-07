import os, io, json, datetime, random, time
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request

# ====== Config ======
DURATION_SEC = 480
DRIVE_ROOT_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID")
SHEET_ID = os.environ.get("SHEET_ID")  # reservado para próximas etapas (opcional)

FOLDER_NAMES = [
    "00_config","01_assets_brolls","01_assets_imagens_jesus","01_assets_imagens_maria",
    "01_assets_musicas","01_assets_musicas_ave_maria","02_scripts_autogerados",
    "03_outputs_videos_pt","03_outputs_videos_en","03_outputs_videos_es","03_outputs_videos_pl",
    "04_outputs_thumbnails_pt","04_outputs_thumbnails_en","04_outputs_thumbnails_es","04_outputs_thumbnails_pl",
    "05_logs","06_backups"
]

# ====== OAuth (refresh token do usuário dono do Drive) ======
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
]

def build_services_from_oauth():
    cid  = os.environ.get("OAUTH_CLIENT_ID")
    csec = os.environ.get("OAUTH_CLIENT_SECRET")
    rtok = os.environ.get("OAUTH_REFRESH_TOKEN")
    if not (cid and csec and rtok):
        raise RuntimeError("Credenciais OAuth ausentes: defina OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET e OAUTH_REFRESH_TOKEN.")
    creds = Credentials(
        token=None,
        refresh_token=rtok,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=cid,
        client_secret=csec,
        scopes=SCOPES,
    )
    creds.refresh(Request())
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets

# ====== Drive helpers ======
def list_children(service, parent_id, mime=None, name=None, order="modifiedTime desc"):
    q = f"'{parent_id}' in parents and trashed=false"
    if mime:
        q += f" and mimeType='{mime}'"
    if name:
        q += f" and name='{name}'"
    res = service.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime,size)",
        orderBy=order,
        pageSize=200
    ).execute()
    return res.get("files", [])

def find_child_by_name(service, parent_id, name):
    items = list_children(service, parent_id, name=name)
    return items[0] if items else None

def ensure_folder(service, parent_id, name):
    f = find_child_by_name(service, parent_id, name)
    if f: return f
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    return service.files().create(body=meta, fields="id,name").execute()

def download_text(service, file_id, encoding="utf-8"):
    fh = io.BytesIO()
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue().decode(encoding, errors="ignore")

def upload_text(service, parent_id, name, text):
    # remove anterior com mesmo nome (evita lixo)
    prev = list_children(service, parent_id, name=name)
    for p in prev:
        service.files().delete(fileId=p["id"]).execute()
    media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")), mimetype="text/plain")
    meta = {"name": name, "parents": [parent_id]}
    return service.files().create(body=meta, media_body=media, fields="id,name").execute()

# ====== Logs ======
def write_log(service, logs_folder_id, lines):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    content = "\n".join(lines)
    upload_text(service, logs_folder_id, f"log_worker_{ts}.txt", content)

# ====== Main flow (validação end-to-end) ======
def main():
    drive_svc, _ = build_services_from_oauth()

    if not DRIVE_ROOT_ID:
        raise RuntimeError("DRIVE_ROOT_FOLDER_ID não definido no ambiente do job.")

    # Garantir estrutura
    folders = {}
    for name in FOLDER_NAMES:
        folders[name] = ensure_folder(drive_svc, DRIVE_ROOT_ID, name)["id"]

    lines = []
    lines.append(f"[{datetime.datetime.utcnow().isoformat()}Z] Worker iniciado.")
    lines.append(f"Root: {DRIVE_ROOT_ID}")

    # 1) Pega a última ordem de serviço
    cfg_id = folders["00_config"]
    orders = [f for f in list_children(drive_svc, cfg_id) if f["name"].startswith("work_orders_") and f["name"].endswith(".json")]
    if not orders:
        lines.append("Nenhuma work_orders_*.json encontrada. Finalizando.")
        write_log(drive_svc, folders["05_logs"], lines)
        print("\n".join(lines))
        return
    latest = sorted(orders, key=lambda x: x["modifiedTime"], reverse=True)[0]
    lines.append(f"Ordem de serviço: {latest['name']}")

    # 2) Ler JSON
    try:
        payload = json.loads(download_text(drive_svc, latest["id"]))
    except Exception as e:
        lines.append(f"[ERRO] Falha lendo JSON: {e}")
        write_log(drive_svc, folders["05_logs"], lines)
        print("\n".join(lines))
        return

    if not isinstance(payload, list) or not payload:
        lines.append("[ERRO] JSON vazio ou inválido.")
        write_log(drive_svc, folders["05_logs"], lines)
        print("\n".join(lines))
        return

    # 3) Validar para cada job: TSV, imagens, música e destinos
    scripts_id = folders["02_scripts_autogerados"]
    imgs_jesus = list_children(drive_svc, folders["01_assets_imagens_jesus"])
    imgs_maria = list_children(drive_svc, folders["01_assets_imagens_maria"])
    music_bg   = list_children(drive_svc, folders["01_assets_musicas"])
    music_ave  = list_children(drive_svc, folders["01_assets_musicas_ave_maria"])

    for job in payload:
        idioma      = job.get("idioma")
        slot        = job.get("slot")
        policy      = job.get("policy")
        faixa       = job.get("faixa_ave_maria","")
        personagem  = "jesus" if slot.startswith("jesus_") else "maria"
        lines.append(f"→ Job {idioma} | {slot} | policy={policy}")

        # TSV
        tsv_name = f"run_{slot}.tsv"
        tsv = find_child_by_name(drive_svc, scripts_id, tsv_name)
        if not tsv:
            lines.append(f"   [ERRO] TSV não encontrado: {tsv_name}")
            continue
        tsv_text = download_text(drive_svc, tsv["id"]).strip()
        if not tsv_text:
            lines.append(f"   [ERRO] TSV vazio: {tsv_name}")
            continue
        linhas = [l.split("\t") for l in tsv_text.split("\n")]
        lines.append(f"   TSV OK: {tsv_name} ({len(linhas)} linhas)")

        # imagem
        pool = imgs_jesus if personagem=="jesus" else imgs_maria
        if not pool:
            lines.append(f"   [ERRO] Nenhuma imagem em 01_assets_imagens_{personagem}")
            continue
        lines.append(f"   Imagem OK: {len(pool)} arquivos")

        # música
        if policy == "ave_maria":
            if faixa:
                alvo = find_child_by_name(drive_svc, folders["01_assets_musicas_ave_maria"], faixa)
                if not alvo:
                    lines.append(f"   [ERRO] Faixa Ave Maria informada mas não encontrada: {faixa}")
                    continue
                lines.append(f"   Música OK (ave_maria): {faixa}")
            else:
                if not music_ave:
                    lines.append("   [ERRO] Pasta 01_assets_musicas_ave_maria vazia.")
                    continue
                lines.append("   Música OK (ave_maria): default da pasta")
        else:
            if not music_bg:
                lines.append("   [ERRO] Pasta 01_assets_musicas vazia.")
                continue
            lines.append(f"   Música OK (bg_random): {len(music_bg)} faixas")

        # destinos
        out_video = {
            "pt":"03_outputs_videos_pt","en":"03_outputs_videos_en",
            "es":"03_outputs_videos_es","pl":"03_outputs_videos_pl"
        }.get(idioma,"03_outputs_videos_pt")
        out_thumb = {
            "pt":"04_outputs_thumbnails_pt","en":"04_outputs_thumbnails_en",
            "es":"04_outputs_thumbnails_es","pl":"04_outputs_thumbnails_pl"
        }.get(idioma,"04_outputs_thumbnails_pt")
        lines.append(f"   Destinos OK: {out_video}, {out_thumb}")

    # 4) Registrar log
    write_log(drive_svc, folders["05_logs"], lines)
    print("\n".join(lines))

if __name__ == "__main__":
    main()

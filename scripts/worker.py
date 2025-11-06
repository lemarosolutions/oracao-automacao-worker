import os, io, json, random, time, datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# ====== Config ======
DRIVE_ROOT_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID")
SA_FILE = os.environ.get("SERVICE_ACCOUNT_FILE")  # service_account.json
DURATION_SEC = 480
FOLDER_NAMES = [
    "00_config","01_assets_brolls","01_assets_imagens_jesus","01_assets_imagens_maria",
    "01_assets_musicas","01_assets_musicas_ave_maria","02_scripts_autogerados",
    "03_outputs_videos_pt","03_outputs_videos_en","03_outputs_videos_es","03_outputs_videos_pl",
    "04_outputs_thumbnails_pt","04_outputs_thumbnails_en","04_outputs_thumbnails_es","04_outputs_thumbnails_pl",
    "05_logs","06_backups"
]

# ====== Drive helpers ======
def drive():
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def list_children(service, parent_id, mime=None, name=None, order="modifiedTime desc"):
    q = f"'{parent_id}' in parents and trashed=false"
    if mime:
        q += f" and mimeType='{mime}'"
    if name:
        # name exact match
        q += f" and name='{name}'"
    res = service.files().list(q=q, fields="files(id,name,mimeType,modifiedTime,size)", orderBy=order, pageSize=200).execute()
    return res.get("files", [])

def find_child_by_name(service, parent_id, name):
    items = list_children(service, parent_id, name=name)
    return items[0] if items else None

def ensure_folder(service, parent_id, name):
    f = find_child_by_name(service, parent_id, name)
    if f: return f
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    f = service.files().create(body=meta, fields="id,name").execute()
    return f

def download_text(service, file_id, encoding="utf-8"):
    fh = io.BytesIO()
    request = service.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue().decode(encoding, errors="ignore")

def upload_text(service, parent_id, name, text):
    # remove previous with same name
    prev = list_children(service, parent_id, name=name)
    for p in prev:
        service.files().delete(fileId=p["id"]).execute()
    media = MediaIoBaseUpload(io.BytesIO(text.encode("utf-8")), mimetype="text/plain")
    meta = {"name": name, "parents": [parent_id]}
    return service.files().create(body=meta, media_body=media, fields="id,name").execute()

# ====== Logs ======
def log(service, logs_folder_id, lines):
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    content = "\n".join(lines)
    upload_text(service, logs_folder_id, f"log_worker_{ts}.txt", content)

# ====== Main flow (validação) ======
def main():
    svc = drive()
    if not DRIVE_ROOT_ID:
        raise RuntimeError("DRIVE_ROOT_FOLDER_ID não definido")

    # Garantir estrutura e apontar IDs úteis
    folders = {}
    for name in FOLDER_NAMES:
        folders[name] = ensure_folder(svc, DRIVE_ROOT_ID, name)["id"]

    lines = []
    lines.append(f"[{datetime.datetime.utcnow().isoformat()}Z] Worker iniciado.")
    lines.append(f"Root: {DRIVE_ROOT_ID}")

    # 1) Localizar a última ordem de serviço em 00_config
    cfg_id = folders["00_config"]
    orders = [f for f in list_children(svc, cfg_id) if f["name"].startswith("work_orders_") and f["name"].endswith(".json")]
    if not orders:
        lines.append("Nenhuma work_orders_*.json encontrada. Finalizando.")
        log(svc, folders["05_logs"], lines)
        return
    latest = sorted(orders, key=lambda x: x["modifiedTime"], reverse=True)[0]
    lines.append(f"Ordem de serviço: {latest['name']}")

    # 2) Ler JSON
    payload = json.loads(download_text(svc, latest["id"]))
    if not isinstance(payload, list) or not payload:
        lines.append("JSON vazio ou inválido.")
        log(svc, folders["05_logs"], lines)
        return

    # 3) Para cada job, checar TSV e assets mínimos
    scripts_id = folders["02_scripts_autogerados"]
    imgs_jesus = list_children(svc, folders["01_assets_imagens_jesus"])
    imgs_maria = list_children(svc, folders["01_assets_imagens_maria"])
    music_bg = list_children(svc, folders["01_assets_musicas"])
    music_ave = list_children(svc, folders["01_assets_musicas_ave_maria"])

    for job in payload:
        idioma = job.get("idioma")
        slot = job.get("slot")
        policy = job.get("policy")
        faixa = job.get("faixa_ave_maria","")
        personagem = "jesus" if slot.startswith("jesus_") else "maria"
        lines.append(f"→ Job {idioma} | {slot} | policy={policy}")

        # TSV
        tsv_name = f"run_{slot}.tsv"
        tsv = find_child_by_name(svc, scripts_id, tsv_name)
        if not tsv:
            lines.append(f"   [ERRO] TSV não encontrado: {tsv_name}")
            continue
        tsv_text = download_text(svc, tsv["id"])
        if not tsv_text.strip():
            lines.append(f"   [ERRO] TSV vazio: {tsv_name}")
            continue
        linhas = [l.split("\t") for l in tsv_text.strip().split("\n")]
        lines.append(f"   TSV OK: {tsv_name} ({len(linhas)} linhas)")

        # imagem
        pool = imgs_jesus if personagem=="jesus" else imgs_maria
        if not pool:
            lines.append(f"   [ERRO] Nenhuma imagem em 01_assets_imagens_{personagem}")
            continue
        lines.append(f"   Imagem OK: {len(pool)} arquivos disponíveis")

        # música
        if policy == "ave_maria":
            if faixa:
                alvo = find_child_by_name(svc, folders["01_assets_musicas_ave_maria"], faixa)
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
            lines.append(f"   Música OK (bg_random): {len(music_bg)} faixas disponíveis")

        # destino de saída
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
    log(svc, folders["05_logs"], lines)
    print("\n".join(lines))

if __name__ == "__main__":
    main()

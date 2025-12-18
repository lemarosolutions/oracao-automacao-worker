# scripts/renderer.py
# Renderer SAFE + Queue-Aware + Idempotente
# - Roda em cron frequente sem duplicar vídeos
# - Processa apenas jobs com publishAt dentro da janela (HORIZON_HOURS)
# - Usa Service Account (SERVICE_ACCOUNT_JSON) como prioridade
# - Mantém slideshow estável por concat (sem xfade)
#
# ENV:
# - DRIVE_ROOT_FOLDER_ID
# - SERVICE_ACCOUNT_JSON  (conteúdo do JSON)
# - (legado) OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET / OAUTH_REFRESH_TOKEN (fallback)
# - HORIZON_HOURS (default 12)
#
# Drive folders esperadas:
# 00_config (work_orders_*.json)
# 02_scripts_autogerados (TSVs)
# 03_outputs_videos_{pt|en|es|pl}
# 04_outputs_thumbnails_{pt|en|es|pl}
# 05_logs
# 01_assets_imagens_jesus / 01_assets_imagens_maria / 01_assets_brolls
# 01_assets_musicas / 01_assets_musicas_ave_maria

import os, io, json, random, tempfile, shutil, re, math, argparse
from datetime import datetime, timezone
import subprocess as sp

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

from PIL import Image, ImageDraw, ImageFont

# -------------------- CONFIG --------------------
TARGET_SEC_DEFAULT = 480
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
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# -------------------- SHELL ---------------------
def sh(cmd: str) -> str:
    cp = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
    if cp.returncode != 0:
        raise RuntimeError(cp.stdout)
    return cp.stdout

def ffprobe_duration(path: str) -> float:
    out = sh(f'ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "{path}"')
    return float(out.strip())

def to_str(v) -> str:
    if v is None:
        return ""
    return str(v).strip()

def safe_slug(s: str) -> str:
    s = re.sub(r'[^a-zA-Z0-9_\-\.]+', '_', to_str(s))
    s = re.sub(r'_+', '_', s).strip('_')
    return s[:120] if s else "job"

def parse_iso_utc(s: str):
    """
    Aceita '2025-01-01T03:41:00Z' ou '...+00:00' etc.
    Retorna datetime UTC ou None.
    """
    s = to_str(s)
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# -------------------- AUTH ----------------------
def build_drive_service():
    sa_json = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
        return build("drive", "v3", credentials=creds)

    # fallback OAuth (legado)
    client_id = os.getenv("OAUTH_CLIENT_ID", "").strip()
    client_secret = os.getenv("OAUTH_CLIENT_SECRET", "").strip()
    refresh_token = os.getenv("OAUTH_REFRESH_TOKEN", "").strip()
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError("Credenciais ausentes: defina SERVICE_ACCOUNT_JSON (preferencial) ou OAuth legado.")

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

# -------------------- DRIVE HELPERS -------------
def list_by_name(svc, parent_id: str, name: str):
    q = f"'{parent_id}' in parents and trashed=false and name='{name}'"
    r = svc.files().list(q=q, fields="files(id,name)").execute()
    return r.get("files", [])

def ensure_folder(svc, parent_id: str, name: str) -> str:
    r = list_by_name(svc, parent_id, name)
    if r:
        return r[0]["id"]
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    return svc.files().create(body=meta, fields="id").execute()["id"]

def list_files_in_folder(svc, folder_id: str, page_size: int = 1000):
    q = f"'{folder_id}' in parents and trashed=false"
    r = svc.files().list(q=q, fields="files(id,name,mimeType,size,modifiedTime)", pageSize=page_size).execute()
    return r.get("files", [])

def file_exists_by_name_contains(svc, folder_id: str, needle: str) -> bool:
    needle = to_str(needle)
    if not needle:
        return False
    # busca por "name contains"
    q = f"'{folder_id}' in parents and trashed=false and name contains '{needle}'"
    r = svc.files().list(q=q, fields="files(id,name)", pageSize=1).execute()
    return bool(r.get("files"))

def download_text(svc, file_id: str) -> str:
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    return buf.getvalue().decode("utf-8")

def download_binary(svc, file_id: str, out_path: str):
    req = svc.files().get_media(fileId=file_id)
    with open(out_path, "wb") as out:
        dl = MediaIoBaseDownload(out, req)
        done = False
        while not done:
            _, done = dl.next_chunk()

def upload_file(svc, parent_id: str, local_path: str, name: str, mime: str) -> str:
    meta = {"name": name, "parents": [parent_id]}
    media = MediaIoBaseUpload(open(local_path, "rb"), mimetype=mime, resumable=True)
    return svc.files().create(body=meta, media_body=media, fields="id").execute()["id"]

def pick_random_local(svc, folder_id: str, exts, avoid_names=None):
    avoid_names = set([n.lower() for n in (avoid_names or [])])
    files = list_files_in_folder(svc, folder_id)
    cand = [f for f in files if any(f["name"].lower().endswith(e) for e in exts) and f["name"].lower() not in avoid_names]
    if not cand:
        cand = [f for f in files if any(f["name"].lower().endswith(e) for e in exts)]
    if not cand:
        return None, None
    f = random.choice(cand)
    fd, tmp = tempfile.mkstemp(suffix="_" + f["name"]); os.close(fd)
    download_binary(svc, f["id"], tmp)
    return tmp, f["name"]

def download_many_images(svc, folder_id: str, limit: int, avoid_names=None):
    avoid_names = set([n.lower() for n in (avoid_names or [])])
    files = list_files_in_folder(svc, folder_id)
    imgs = [f for f in files if any(f["name"].lower().endswith(e) for e in IMG_EXTS)]
    random.shuffle(imgs)

    chosen = [f for f in imgs if f["name"].lower() not in avoid_names][:limit]
    if len(chosen) < max(1, min(limit, 5)):
        chosen += [f for f in imgs if f not in chosen][: (limit - len(chosen))]

    paths, names = [], []
    for f in chosen:
        fd, tmp = tempfile.mkstemp(suffix="_" + f["name"]); os.close(fd)
        download_binary(svc, f["id"], tmp)
        paths.append(tmp); names.append(f["name"])
    return paths, names

# -------------------- STATE ---------------------
def load_state(svc, cfg_id: str):
    r = list_by_name(svc, cfg_id, "state.json")
    if not r:
        return {"recent_images": {}, "recent_music": {}}, None
    fid = r[0]["id"]
    try:
        data = json.loads(download_text(svc, fid))
        if not isinstance(data, dict):
            data = {"recent_images": {}, "recent_music": {}}
        data.setdefault("recent_images", {})
        data.setdefault("recent_music", {})
        return data, fid
    except:
        return {"recent_images": {}, "recent_music": {}}, fid

def save_state(svc, cfg_id: str, state: dict, existing_file_id):
    tmp = tempfile.mkstemp(suffix="_state.json")[1]
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    if existing_file_id:
        media = MediaIoBaseUpload(open(tmp, "rb"), mimetype="application/json", resumable=True)
        svc.files().update(fileId=existing_file_id, media_body=media).execute()
    else:
        upload_file(svc, cfg_id, tmp, "state.json", "application/json")
    os.remove(tmp)

def push_recent(lst: list, item: str, max_len: int):
    if not item:
        return lst
    item_l = item.lower()
    lst = [x for x in lst if x.lower() != item_l]
    lst.insert(0, item)
    return lst[:max_len]

# -------------------- TSV -----------------------
def load_tsv_rows(tsv_path: str):
    rows = []
    with open(tsv_path, "r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if not ln:
                continue
            parts = ln.split("\t")
            head0 = parts[0].strip().lower()
            if head0 in ("run", "passo_ordem"):
                continue

            if len(parts) >= 4:
                try: ord_ = int(parts[1])
                except: continue
                tipo = to_str(parts[2]).lower()
                txt  = to_str(parts[3])
            elif len(parts) >= 3:
                try: ord_ = int(parts[0])
                except: continue
                tipo = to_str(parts[1]).lower()
                txt  = to_str(parts[2])
            else:
                continue

            rows.append({"ord": ord_, "tipo": tipo, "txt": txt})

    rows.sort(key=lambda x: x["ord"])
    return rows

def narration_from_rows(rows):
    texts = []
    policy = "bg_random"
    faixa_ave_maria = ""

    for r in rows:
        t = r["tipo"]
        if t == "musica_policy":
            policy = to_str(r["txt"]).lower()
            continue
        if t == "faixa_ave_maria":
            faixa_ave_maria = to_str(r["txt"])
            continue
        if t in SAY_TYPES:
            val = to_str(r["txt"])
            if val:
                texts.append(val)

    base = " ".join(texts).strip()
    if not base:
        base = "Oração de paz e esperança. Que Deus abençoe o seu dia."

    # booster se curto
    words = base.split()
    if len(words) < 700:
        rep = max(1, math.ceil(900 / max(1, len(words))))
        base = (" " + (base + " ")).join([""] * rep).strip()

    return base, policy, faixa_ave_maria

# -------------------- THUMB ---------------------
def make_thumb(base_img, title, out_jpg):
    img = Image.open(base_img).convert("RGB").resize((W, H))
    draw = ImageDraw.Draw(img)

    overlay = Image.new("RGBA", img.size, (0, 0, 0, 140))
    img = Image.alpha_composite(img.convert("RGBA"), overlay)

    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", 88)
    except:
        font = ImageFont.load_default()

    words = to_str(title)[:70].split()
    lines, line = [], ""
    for w in words:
        test = (line + " " + w).strip()
        if len(test) <= 22:
            line = test
        else:
            if line:
                lines.append(line)
            line = w
        if len(lines) >= 3:
            break
    if line and len(lines) < 3:
        lines.append(line)

    y = H // 2 - (len(lines) * 90) // 2
    for ln in lines:
        wtxt, _ = draw.textsize(ln, font=font)
        x = (W - wtxt) // 2
        draw.text((x, y), ln, font=font, fill=(255, 255, 255, 240))
        y += 100

    img.convert("RGB").save(out_jpg, "JPEG", quality=92)

# -------------------- TTS -----------------------
def build_tts_wav(text: str, out_wav: str, lang: str):
    """
    Preferência edge-tts (se existir), fallback gTTS.
    """
    text = text.replace('"', "")

    # edge-tts se disponível
    try:
        sh("python -c \"import edge_tts\"")
        voice_map = {
            "pt": "pt-BR-AntonioNeural",
            "en": "en-US-GuyNeural",
            "es": "es-MX-JorgeNeural",
            "pl": "pl-PL-MarekNeural",
        }
        voice = voice_map.get(lang, "pt-BR-AntonioNeural")
        tmp_mp3 = out_wav.replace(".wav", ".mp3")
        sh(f'python -m edge_tts --voice "{voice}" --text "{text}" --write-media "{tmp_mp3}"')
        sh(f'ffmpeg -y -i "{tmp_mp3}" -ac 1 -ar 44100 "{out_wav}"')
        os.remove(tmp_mp3)
        return
    except Exception:
        pass

    # gTTS fallback
    from gtts import gTTS
    tmp_mp3 = out_wav.replace(".wav", ".mp3")
    gTTS(text=text, lang=("pt" if lang == "pt" else lang), slow=False).save(tmp_mp3)
    sh(f'ffmpeg -y -i "{tmp_mp3}" -ac 1 -ar 44100 "{out_wav}"')
    os.remove(tmp_mp3)

# -------------------- AUDIO MIX -----------------
def mix_voice_and_music(voice_wav: str, music_path: str | None, out_wav: str, target_sec: int):
    if not music_path:
        sh(f'ffmpeg -y -i "{voice_wav}" -t {target_sec} -af "apad=pad_dur={target_sec}" "{out_wav}"')
        return

    sh(
        f'ffmpeg -y -stream_loop -1 -i "{music_path}" -i "{voice_wav}" '
        f'-filter_complex '
        f'"[0:a]volume=0.18,aloop=loop=-1:size=2e6[a0];'
        f'[a0]atrim=0:{target_sec}[m];'
        f'[1:a]atrim=0:{target_sec}[v];'
        f'[v][m]amix=inputs=2:normalize=0[a]" '
        f'-map "[a]" -t {target_sec} "{out_wav}"'
    )

# -------------------- VIDEO SAFE ---------------
def build_slideshow_concat(img_paths: list[str], dur_sec: float, out_mp4: str):
    if not img_paths:
        raise RuntimeError("Sem imagens para slideshow.")
    per = max(3.0, dur_sec / len(img_paths))
    inputs = " ".join([f'-loop 1 -t {per:.3f} -i "{p}"' for p in img_paths])

    prep = ";".join([
        f'[{i}:v]scale={W}:{H},fps={FPS},format=yuv420p,setpts=PTS-STARTPTS[v{i}]'
        for i in range(len(img_paths))
    ])
    concat_inputs = "".join([f"[v{i}]" for i in range(len(img_paths))])
    chain = f"{prep};{concat_inputs}concat=n={len(img_paths)}:v=1:a=0[outv]"

    sh(
        f'ffmpeg -y {inputs} -filter_complex "{chain}" '
        f'-map [outv] -r {FPS} -pix_fmt yuv420p -movflags +faststart -t {dur_sec:.3f} "{out_mp4}"'
    )

# -------------------- WORK ORDERS ---------------
def normalize_jobs(raw):
    """
    Aceita:
    - lista de dicts
    - dict com chave 'orders'
    - string JSON
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

def get_latest_work_orders(svc, cfg_id: str):
    q = f"'{cfg_id}' in parents and trashed=false and name contains 'work_orders_'"
    r = svc.files().list(q=q, orderBy="modifiedTime desc", pageSize=1, fields="files(id,name)").execute()
    if not r.get("files"):
        raise RuntimeError("Nenhum work_orders_*.json encontrado em 00_config.")
    fid = r["files"][0]["id"]
    raw = json.loads(download_text(svc, fid))
    return normalize_jobs(raw), r["files"][0]["name"]

# -------------------- MAIN ----------------------
def preflight():
    sh("ffmpeg -version")
    sh("ffprobe -version")

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--duration", type=int, default=TARGET_SEC_DEFAULT)
    args, _ = parser.parse_known_args()

    target_sec = int(args.duration or TARGET_SEC_DEFAULT)
    horizon_hours = int(to_str(os.getenv("HORIZON_HOURS", "12")) or "12")

    svc = build_drive_service()
    ROOT = to_str(os.getenv("DRIVE_ROOT_FOLDER_ID"))
    if not ROOT:
        raise RuntimeError("DRIVE_ROOT_FOLDER_ID não definido.")

    preflight()

    # Pastas
    cfg_id     = ensure_folder(svc, ROOT, "00_config")
    scripts_id = ensure_folder(svc, ROOT, "02_scripts_autogerados")
    logs_id    = ensure_folder(svc, ROOT, "05_logs")

    out_ids = {
        "pt": ensure_folder(svc, ROOT, "03_outputs_videos_pt"),
        "en": ensure_folder(svc, ROOT, "03_outputs_videos_en"),
        "es": ensure_folder(svc, ROOT, "03_outputs_videos_es"),
        "pl": ensure_folder(svc, ROOT, "03_outputs_videos_pl"),
    }
    th_ids = {
        "pt": ensure_folder(svc, ROOT, "04_outputs_thumbnails_pt"),
        "en": ensure_folder(svc, ROOT, "04_outputs_thumbnails_en"),
        "es": ensure_folder(svc, ROOT, "04_outputs_thumbnails_es"),
        "pl": ensure_folder(svc, ROOT, "04_outputs_thumbnails_pl"),
    }

    img_jesus = ensure_folder(svc, ROOT, "01_assets_imagens_jesus")
    img_maria = ensure_folder(svc, ROOT, "01_assets_imagens_maria")
    brolls    = ensure_folder(svc, ROOT, "01_assets_brolls")
    mus_bg    = ensure_folder(svc, ROOT, "01_assets_musicas")
    mus_am    = ensure_folder(svc, ROOT, "01_assets_musicas_ave_maria")

    # Estado anti-repetição
    state, state_fid = load_state(svc, cfg_id)

    # Work orders
    jobs, wo_name = get_latest_work_orders(svc, cfg_id)

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc + (timezone.utc.utcoffset(now_utc) or now_utc.utcoffset() or 0)  # noop safe
    window_end = now_utc + (datetime.now(timezone.utc) - datetime.now(timezone.utc))  # zero, safe
    window_end = now_utc  # reset
    window_end = now_utc + (timezone.utc.utcoffset(now_utc) or datetime.now(timezone.utc).utcoffset() or (now_utc - now_utc))
    # simpler:
    window_end = now_utc + (datetime.fromtimestamp(0, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))
    window_end = now_utc + (now_utc - now_utc)
    window_end = now_utc  # final reset
    window_end = now_utc + (datetime.timedelta(hours=horizon_hours) if hasattr(datetime, "timedelta") else None)

# Fix timedelta import safely
from datetime import timedelta

def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--duration", type=int, default=TARGET_SEC_DEFAULT)
    args, _ = parser.parse_known_args()

    target_sec = int(args.duration or TARGET_SEC_DEFAULT)
    horizon_hours = int(to_str(os.getenv("HORIZON_HOURS", "12")) or "12")

    svc = build_drive_service()
    ROOT = to_str(os.getenv("DRIVE_ROOT_FOLDER_ID"))
    if not ROOT:
        raise RuntimeError("DRIVE_ROOT_FOLDER_ID não definido.")

    preflight()

    cfg_id     = ensure_folder(svc, ROOT, "00_config")
    scripts_id = ensure_folder(svc, ROOT, "02_scripts_autogerados")
    logs_id    = ensure_folder(svc, ROOT, "05_logs")

    out_ids = {
        "pt": ensure_folder(svc, ROOT, "03_outputs_videos_pt"),
        "en": ensure_folder(svc, ROOT, "03_outputs_videos_en"),
        "es": ensure_folder(svc, ROOT, "03_outputs_videos_es"),
        "pl": ensure_folder(svc, ROOT, "03_outputs_videos_pl"),
    }
    th_ids = {
        "pt": ensure_folder(svc, ROOT, "04_outputs_thumbnails_pt"),
        "en": ensure_folder(svc, ROOT, "04_outputs_thumbnails_en"),
        "es": ensure_folder(svc, ROOT, "04_outputs_thumbnails_es"),
        "pl": ensure_folder(svc, ROOT, "04_outputs_thumbnails_pl"),
    }

    img_jesus = ensure_folder(svc, ROOT, "01_assets_imagens_jesus")
    img_maria = ensure_folder(svc, ROOT, "01_assets_imagens_maria")
    brolls    = ensure_folder(svc, ROOT, "01_assets_brolls")
    mus_bg    = ensure_folder(svc, ROOT, "01_assets_musicas")
    mus_am    = ensure_folder(svc, ROOT, "01_assets_musicas_ave_maria")

    state, state_fid = load_state(svc, cfg_id)
    jobs, wo_name = get_latest_work_orders(svc, cfg_id)

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc + timedelta(hours=horizon_hours)

    tmpdir = tempfile.mkdtemp()
    log_lines = [f"UTC:{now_utc.isoformat()} work_orders:{wo_name} horizon_hours:{horizon_hours}"]

    try:
        processed = 0
        skipped = 0

        for idx, job in enumerate(jobs):
            lang = to_str(job.get("idioma") or job.get("lang") or "pt").lower()
            slot = to_str(job.get("slot"))
            title = to_str(job.get("title") or job.get("titulo") or slot)

            publish_at = to_str(job.get("publishAt") or job.get("publish_at") or job.get("publish_at_utc"))
            dt_pub = parse_iso_utc(publish_at)

            # Sem publishAt => não entra na fila (evita comportamento imprevisível)
            if not dt_pub:
                skipped += 1
                log_lines.append(f"[SKIP] sem publishAt slot={slot} lang={lang}")
                continue

            # Fora da janela => não renderiza agora
            if not (now_utc <= dt_pub <= window_end):
                skipped += 1
                log_lines.append(f"[SKIP] fora janela publishAt={dt_pub.isoformat()} slot={slot} lang={lang}")
                continue

            # job_id estável
            job_id = to_str(job.get("job_id") or job.get("id") or "")
            if not job_id:
                job_id = f"{slot}_{lang}_{dt_pub.strftime('%Y%m%d_%H%M')}_{idx}"
            job_id = safe_slug(job_id)

            # Idempotência: se já existe mp4 com job_id no folder de saída, pula
            out_folder = out_ids.get(lang, out_ids["pt"])
            if file_exists_by_name_contains(svc, out_folder, job_id):
                skipped += 1
                log_lines.append(f"[SKIP] já existe output job_id={job_id} slot={slot} lang={lang}")
                continue

            # Resolve TSV: run_{slot}_{lang}.tsv ou run_{slot}.tsv
            candidates = [f"run_{slot}_{lang}.tsv", f"run_{slot}.tsv"]
            tsv_file_id = None
            for nm in candidates:
                rs = list_by_name(svc, scripts_id, nm)
                if rs:
                    tsv_file_id = rs[0]["id"]
                    break
            if not tsv_file_id:
                skipped += 1
                log_lines.append(f"[SKIP] TSV não encontrado slot={slot} lang={lang}")
                continue

            # Baixa TSV
            tsv_local = os.path.join(tmpdir, f"run_{slot}_{lang}.tsv")
            download_binary(svc, tsv_file_id, tsv_local)

            rows = load_tsv_rows(tsv_local)
            narr_text, pol_from_tsv, faixa_ave_maria_tsv = narration_from_rows(rows)

            musica_policy = to_str(job.get("musica_policy") or job.get("policy") or pol_from_tsv or "bg_random").lower()
            faixa_job = to_str(job.get("faixa_ave_maria"))
            faixa_ave = faixa_job or faixa_ave_maria_tsv

            # TTS
            voice_wav = os.path.join(tmpdir, f"voice_{job_id}.wav")
            build_tts_wav(narr_text, voice_wav, lang)
            voice_len = ffprobe_duration(voice_wav)

            # Imagens (anti repetição por idioma)
            state.setdefault("recent_images", {})
            state.setdefault("recent_music", {})
            recent_imgs = state["recent_images"].get(lang, [])

            base_folder = img_maria if "maria" in slot else img_jesus
            img_paths, img_names = download_many_images(svc, base_folder, limit=20, avoid_names=recent_imgs)
            if len(img_paths) < 1:
                img_paths, img_names = download_many_images(svc, brolls, limit=10, avoid_names=recent_imgs)
            if len(img_paths) < 1:
                raise RuntimeError("Sem imagens disponíveis (assets).")

            # Vídeo
            base_dur = min(max(voice_len, MIN_SLIDESHOW_SEC), target_sec)
            vid_mp4 = os.path.join(tmpdir, f"slideshow_{job_id}.mp4")
            build_slideshow_concat(img_paths, base_dur, vid_mp4)

            # Música (anti repetição)
            recent_music = state["recent_music"].get(lang, [])
            music_path = None
            music_name = None

            if slot == "maria_v2" and musica_policy == "ave_maria":
                if faixa_ave:
                    cand = list_by_name(svc, mus_am, faixa_ave)
                    if cand:
                        fd, music_path = tempfile.mkstemp(suffix="_" + faixa_ave); os.close(fd)
                        download_binary(svc, cand[0]["id"], music_path)
                        music_name = faixa_ave
                if not music_path:
                    music_path, music_name = pick_random_local(svc, mus_am, AUD_EXTS, avoid_names=recent_music)
            else:
                music_path, music_name = pick_random_local(svc, mus_bg, AUD_EXTS, avoid_names=recent_music)

            # Mix áudio
            mix_wav = os.path.join(tmpdir, f"mix_{job_id}.wav")
            mix_voice_and_music(voice_wav, music_path, mix_wav, target_sec)

            # Mux final (loop vídeo para bater target_sec)
            final_mp4 = os.path.join(tmpdir, f"{job_id}.mp4")
            sh(
                f'ffmpeg -y -stream_loop -1 -i "{vid_mp4}" -i "{mix_wav}" '
                f'-shortest -t {target_sec} '
                f'-map 0:v:0 -map 1:a:0 '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a aac -b:a 160k -pix_fmt yuv420p '
                f'"{final_mp4}"'
            )

            # Thumbnail
            thumb_jpg = os.path.join(tmpdir, f"{job_id}.jpg")
            make_thumb(img_paths[0], title or slot, thumb_jpg)

            # Upload
            upload_file(svc, out_folder, final_mp4, f"{job_id}.mp4", "video/mp4")
            upload_file(svc, th_ids.get(lang, th_ids["pt"]), thumb_jpg, f"{job_id}.jpg", "image/jpeg")

            # Atualiza estado
            state["recent_images"][lang] = state["recent_images"].get(lang, [])
            state["recent_music"][lang] = state["recent_music"].get(lang, [])
            for nm in img_names[:6]:
                state["recent_images"][lang] = push_recent(state["recent_images"][lang], nm, max_len=80)
            if music_name:
                state["recent_music"][lang] = push_recent(state["recent_music"][lang], music_name, max_len=50)

            processed += 1
            log_lines.append(f"[OK] job_id={job_id} slot={slot} lang={lang} publishAt={dt_pub.isoformat()} music={music_name or 'none'}")

        # salva state
        save_state(svc, cfg_id, state, state_fid)

        # log
        logname = f"log_renderer_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.txt"
        txt = "\n".join(log_lines + [f"status:OK processed:{processed} skipped:{skipped}"])
        tmp_log = os.path.join(tmpdir, "log.txt")
        with open(tmp_log, "w", encoding="utf-8") as f:
            f.write(txt)
        upload_file(svc, logs_id, tmp_log, logname, "text/plain")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    main()

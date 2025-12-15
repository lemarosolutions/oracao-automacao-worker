# scripts/renderer.py
# SAFE_MODE renderer: concat estável (sem xfade), fail-fast, pronto para Service Account (Drive/Sheets),
# com fallback para OAuth (se ainda estiver usando).
#
# ENV suportadas:
# - DRIVE_ROOT_FOLDER_ID
# - (Preferencial) SERVICE_ACCOUNT_JSON  -> conteúdo do JSON da service account (copiar/colar no Secret)
#   ou SERVICE_ACCOUNT_FILE              -> caminho local (em runner normalmente não)
# - (Fallback OAuth) OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REFRESH_TOKEN
#
# Saídas:
# - 03_outputs_videos_{pt|en|es|pl}
# - 04_outputs_thumbnails_{pt|en|es|pl}
# - 05_logs/log_renderer_*.txt

import os, io, json, random, tempfile, shutil, re, math
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account

from PIL import Image, ImageDraw, ImageFont
import subprocess as sp

# -------------------- CONFIG --------------------
TARGET_SEC = 480
FPS = 30
W, H = 1920, 1080

SAFE_MODE = True  # concat simples (sem xfade). Mantém pipeline estável.
MIN_SLIDESHOW_SEC = 60.0

IMG_EXTS = ('.jpg', '.jpeg', '.png')
AUD_EXTS = ('.mp3', '.wav', '.m4a', '.aac')

# Tipos que entram na narração (quando TSV tem passo_tipo)
SAY_TYPES = {
    'abertura','exame','suplica','súplica','verso','salmo',
    'meditacao','meditação','meditacoes','meditações',
    'intercessao','intercessão','agradecimento','encerramento',
    'cta','texto','mensagem'
}

# Escopos
SCOPES_SA = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]
SCOPES_OAUTH = SCOPES_SA[:]  # mesma lista

# -------------------- SHELL ---------------------
def sh(cmd: str) -> str:
    cp = sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
    if cp.returncode != 0:
        raise RuntimeError(cp.stdout)
    return cp.stdout

def ffprobe_duration(path: str) -> float:
    out = sh(f'ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "{path}"')
    return float(out.strip())

# -------------------- AUTH ----------------------
def build_drive_service():
    """
    Preferência:
      1) Service Account via SERVICE_ACCOUNT_JSON (conteúdo do JSON)
      2) Service Account via SERVICE_ACCOUNT_FILE (caminho)
      3) OAuth via refresh token
    """
    sa_json = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
    sa_file = os.getenv("SERVICE_ACCOUNT_FILE", "").strip()

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES_SA)
        return build("drive", "v3", credentials=creds)

    if sa_file and os.path.exists(sa_file):
        creds = service_account.Credentials.from_service_account_file(sa_file, scopes=SCOPES_SA)
        return build("drive", "v3", credentials=creds)

    # Fallback OAuth
    client_id = os.getenv("OAUTH_CLIENT_ID")
    client_secret = os.getenv("OAUTH_CLIENT_SECRET")
    refresh_token = os.getenv("OAUTH_REFRESH_TOKEN")
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError("Credenciais ausentes: defina SERVICE_ACCOUNT_JSON (recomendado) ou OAuth (OAUTH_CLIENT_ID/SECRET/REFRESH).")

    creds = Credentials(
        None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES_OAUTH
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

def list_files_in_folder(svc, folder_id: str, page_size: int = 1000):
    q = f"'{folder_id}' in parents and trashed=false"
    r = svc.files().list(q=q, fields="files(id,name,mimeType,size,modifiedTime)", pageSize=page_size).execute()
    return r.get("files", [])

def pick_random_local(svc, folder_id: str, exts, avoid_names=None):
    avoid_names = set([n.lower() for n in (avoid_names or [])])
    files = list_files_in_folder(svc, folder_id)
    cand = [f for f in files if any(f["name"].lower().endswith(e) for e in exts) and f["name"].lower() not in avoid_names]
    if not cand:
        # se não tiver sem repetir, libera repetição
        cand = [f for f in files if any(f["name"].lower().endswith(e) for e in exts)]
    if not cand:
        return None, None
    f = random.choice(cand)
    fd, tmp = tempfile.mkstemp(suffix="_" + f["name"])
    os.close(fd)
    download_binary(svc, f["id"], tmp)
    return tmp, f["name"]

def download_many_images(svc, folder_id: str, limit: int, avoid_names=None):
    avoid_names = set([n.lower() for n in (avoid_names or [])])
    files = list_files_in_folder(svc, folder_id)
    imgs = [f for f in files if any(f["name"].lower().endswith(e) for e in IMG_EXTS)]
    random.shuffle(imgs)

    # prioriza não repetir
    chosen = [f for f in imgs if f["name"].lower() not in avoid_names][:limit]
    if len(chosen) < max(1, min(limit, 5)):
        # se ficou curto, completa com o resto
        extra = [f for f in imgs if f not in chosen][: (limit - len(chosen))]
        chosen += extra

    paths, names = [], []
    for f in chosen:
        fd, tmp = tempfile.mkstemp(suffix="_" + f["name"])
        os.close(fd)
        download_binary(svc, f["id"], tmp)
        paths.append(tmp)
        names.append(f["name"])
    return paths, names

# -------------------- STATE (ANTI-REPETIÇÃO) ----
def load_state(svc, cfg_id: str):
    """
    state.json (Drive/00_config) mantém histórico simples para evitar repetição imediata.
    Estrutura:
      {
        "recent_images": {"pt": [...], "en": [...], ...},
        "recent_music":  {"pt": [...], "en": [...], ...}
      }
    """
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

def save_state(svc, cfg_id: str, state: dict, existing_file_id: str | None):
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

# -------------------- TSV PARSE -----------------
def clean_line(s):
    s = re.sub(r"\[.*?\]", "", str(s))
    return s.strip()

def load_tsv_rows(tsv_path: str):
    """
    Aceita:
      - 4 colunas: run, ord, tipo, txt
      - 3 colunas: passo_ordem, passo_tipo, conteudo_pt
    """
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
                # antigo
                try:
                    ord_ = int(parts[1])
                except:
                    continue
                tipo = parts[2].strip().lower()
                txt = parts[3].strip()
            elif len(parts) >= 3:
                # atual
                try:
                    ord_ = int(parts[0])
                except:
                    continue
                tipo = parts[1].strip().lower()
                txt = parts[2].strip()
            else:
                continue

            rows.append({"ord": ord_, "tipo": tipo, "txt": txt})

    rows.sort(key=lambda x: x["ord"])
    return rows

def narration_from_rows(rows):
    texts = []
    policy = "bg_random"
    faixa_ave_maria = None

    for r in rows:
        t = r["tipo"]
        if t == "musica_policy":
            policy = clean_line(r["txt"]).lower()
            continue
        if t == "faixa_ave_maria":
            faixa_ave_maria = clean_line(r["txt"]).strip()
            continue
        if t in SAY_TYPES:
            val = clean_line(r["txt"])
            if val:
                texts.append(val)

    if not texts:
        texts = [clean_line(r["txt"]) for r in rows if clean_line(r["txt"])]

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

    # overlay escuro para legibilidade
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 140))
    img = Image.alpha_composite(img.convert("RGBA"), overlay)

    # texto (máx 3 linhas)
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", 88)
    except:
        font = ImageFont.load_default()

    words = title.strip()[:70].split()
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
        wtxt, htxt = draw.textsize(ln, font=font)
        x = (W - wtxt) // 2
        draw.text((x, y), ln, font=font, fill=(255, 255, 255, 240))
        y += 100

    img.convert("RGB").save(out_jpg, "JPEG", quality=92)

# -------------------- AUDIO MIX -----------------
def mix_voice_and_music(voice_wav: str, music_path: str | None, out_wav: str, target_sec: int):
    """
    Mix simples e estável:
    - voz em primeiro plano
    - música em volume baixo e loop infinito
    """
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

# -------------------- TTS (ESTÁVEL) -------------
def build_tts_wav(text: str, out_wav: str, lang: str):
    """
    TTS estável sem dependências extras:
    - tenta edge-tts se existir
    - fallback: gTTS
    """
    # 1) edge-tts (se disponível no runner)
    try:
        sh("python -c \"import edge_tts\"")
        # voz padrão por idioma (masculina quando disponível)
        voice_map = {
            "pt": "pt-BR-AntonioNeural",
            "en": "en-US-GuyNeural",
            "es": "es-MX-JorgeNeural",
            "pl": "pl-PL-MarekNeural",
        }
        voice = voice_map.get(lang, "pt-BR-AntonioNeural")
        tmp_mp3 = out_wav.replace(".wav", ".mp3")
        # edge-tts CLI
        sh(f'python -m edge_tts --voice "{voice}" --text "{text.replace(chr(34), "")}" --write-media "{tmp_mp3}"')
        sh(f'ffmpeg -y -i "{tmp_mp3}" -ac 1 -ar 44100 "{out_wav}"')
        os.remove(tmp_mp3)
        return
    except Exception:
        pass

    # 2) gTTS fallback
    from gtts import gTTS
    tmp_mp3 = out_wav.replace(".wav", ".mp3")
    gTTS(text=text, lang=("pt" if lang == "pt" else lang), slow=False).save(tmp_mp3)
    sh(f'ffmpeg -y -i "{tmp_mp3}" -ac 1 -ar 44100 "{out_wav}"')
    os.remove(tmp_mp3)

# -------------------- VIDEO (SAFE) --------------
def build_slideshow_concat(img_paths: list[str], dur_sec: float, out_mp4: str):
    """
    SAFE_MODE: concat por filter_complex (sem xfade).
    """
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

# -------------------- PREFLIGHT -----------------
def preflight(svc, root_id: str):
    # ffmpeg básico
    sh("ffmpeg -version")
    sh("ffprobe -version")

    # root acessível
    svc.files().get(fileId=root_id, fields="id,name").execute()

# -------------------- MAIN ----------------------
def run():
    svc = build_drive_service()
    ROOT = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    if not ROOT:
        raise RuntimeError("DRIVE_ROOT_FOLDER_ID não definido.")

    preflight(svc, ROOT)

    # Pastas do projeto
    cfg_id   = ensure_folder(svc, ROOT, "00_config")
    scripts_id = ensure_folder(svc, ROOT, "02_scripts_autogerados")

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

    logs_id   = ensure_folder(svc, ROOT, "05_logs")

    # Work orders (último)
    q = f"'{cfg_id}' in parents and trashed=false and name contains 'work_orders_'"
    r = svc.files().list(q=q, orderBy="modifiedTime desc", pageSize=1, fields="files(id,name)").execute()
    if not r.get("files"):
        raise RuntimeError("Nenhum work_orders_*.json encontrado em 00_config.")
    wo_id = r["files"][0]["id"]
    raw = json.loads(download_text(svc, wo_id))
    jobs = raw["orders"] if isinstance(raw, dict) and "orders" in raw else (raw if isinstance(raw, list) else [])
    if not jobs:
        raise RuntimeError("work_orders vazio ou inválido.")

    # Estado (anti-repetição simples)
    state, state_fid = load_state(svc, cfg_id)

    tmpdir = tempfile.mkdtemp()
    log_lines = []
    try:
        for job in jobs:
            lang = job.get("idioma", "pt")
            slot = job.get("slot")
            title = job.get("title", "")

            if not slot:
                continue
            if lang not in out_ids:
                lang = "pt"

            musica_policy = (job.get("musica_policy") or job.get("policy") or "bg_random").lower()
            faixa_ave_maria_job = (job.get("faixa_ave_maria") or "").strip()

            # localizar TSV: run_{slot}_{lang}.tsv ou run_{slot}.tsv
            candidates = [f"run_{slot}_{lang}.tsv", f"run_{slot}.tsv"]
            tsv_file = None
            for nm in candidates:
                rs = list_by_name(svc, scripts_id, nm)
                if rs:
                    tsv_file = rs[0]
                    break
            if not tsv_file:
                log_lines.append(f"[SKIP] TSV não encontrado para slot={slot} lang={lang}")
                continue

            tsv_local = os.path.join(tmpdir, f"run_{slot}_{lang}.tsv")
            download_binary(svc, tsv_file["id"], tsv_local)
            rows = load_tsv_rows(tsv_local)
            narr_text, pol_from_tsv, faixa_ave_maria_tsv = narration_from_rows(rows)

            # política final
            if musica_policy == "bg_random":
                musica_policy = pol_from_tsv

            # TTS
            voice_wav = os.path.join(tmpdir, f"voice_{slot}_{lang}.wav")
            build_tts_wav(narr_text, voice_wav, lang)
            voice_len = ffprobe_duration(voice_wav)

            # imagens (anti repetição por idioma)
            recent_imgs = state.get("recent_images", {}).get(lang, [])
            base_folder = img_maria if "maria" in slot else img_jesus

            img_paths, img_names = download_many_images(
                svc,
                base_folder,
                limit=20,
                avoid_names=recent_imgs
            )
            if len(img_paths) < 1:
                # tenta broll
                img_paths, img_names = download_many_images(svc, brolls, limit=10, avoid_names=recent_imgs)

            if len(img_paths) < 1:
                raise RuntimeError("Sem imagens disponíveis (assets).")

            # vídeo slideshow (duração base = voz ou mínimo)
            base_dur = min(max(voice_len, MIN_SLIDESHOW_SEC), TARGET_SEC)
            vid_mp4 = os.path.join(tmpdir, f"slideshow_{slot}_{lang}.mp4")
            build_slideshow_concat(img_paths, base_dur, vid_mp4)

            # música (anti repetição por idioma)
            recent_music = state.get("recent_music", {}).get(lang, [])

            music_path = None
            music_name = None

            if slot == "maria_v2" and musica_policy == "ave_maria":
                # se veio nome explícito, tenta usar
                wanted = (faixa_ave_maria_job or faixa_ave_maria_tsv or "").strip()
                if wanted:
                    cand = list_by_name(svc, mus_am, wanted)
                    if cand:
                        fd, music_path = tempfile.mkstemp(suffix="_" + wanted); os.close(fd)
                        download_binary(svc, cand[0]["id"], music_path)
                        music_name = wanted
                if not music_path:
                    music_path, music_name = pick_random_local(svc, mus_am, AUD_EXTS, avoid_names=recent_music)
            else:
                music_path, music_name = pick_random_local(svc, mus_bg, AUD_EXTS, avoid_names=recent_music)

            # mix final áudio
            mix_wav = os.path.join(tmpdir, f"mix_{slot}_{lang}.wav")
            mix_voice_and_music(voice_wav, music_path, mix_wav, TARGET_SEC)

            # mux final (loop vídeo para bater 480)
            final_mp4 = os.path.join(tmpdir, f"{slot}_{lang}_{datetime.utcnow().strftime('%Y%m%d')}.mp4")
            sh(
                f'ffmpeg -y -stream_loop -1 -i "{vid_mp4}" -i "{mix_wav}" '
                f'-shortest -t {TARGET_SEC} '
                f'-map 0:v:0 -map 1:a:0 '
                f'-c:v libx264 -preset veryfast -crf 20 '
                f'-c:a aac -b:a 160k -pix_fmt yuv420p '
                f'"{final_mp4}"'
            )

            # thumbnail
            thumb_jpg = os.path.join(tmpdir, f"thumb_{slot}_{lang}.jpg")
            make_thumb(img_paths[0], title or slot, thumb_jpg)

            # upload
            upload_file(svc, out_ids[lang], final_mp4, os.path.basename(final_mp4), "video/mp4")
            upload_file(svc, th_ids[lang], thumb_jpg, os.path.basename(thumb_jpg), "image/jpeg")

            # atualiza estado (janela curta para não repetir demais)
            state.setdefault("recent_images", {})
            state.setdefault("recent_music", {})
            state["recent_images"][lang] = state["recent_images"].get(lang, [])
            state["recent_music"][lang] = state["recent_music"].get(lang, [])

            # empurra alguns nomes (imagens e música)
            for nm in img_names[:6]:
                state["recent_images"][lang] = push_recent(state["recent_images"][lang], nm, max_len=60)
            if music_name:
                state["recent_music"][lang] = push_recent(state["recent_music"][lang], music_name, max_len=30)

            log_lines.append(
                f"[OK] slot={slot} lang={lang} "
                f"policy={musica_policy} voice_len={voice_len:.1f}s "
                f"imgs={len(img_names)} music={'none' if not music_name else music_name}"
            )

        # salva estado
        save_state(svc, cfg_id, state, state_fid)

        # log no Drive
        logname = f"log_renderer_{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.txt"
        txt = "UTC:" + datetime.utcnow().isoformat() + "\n" + "\n".join(log_lines) + "\nstatus:OK render_safe\n"
        tmp_log = os.path.join(tmpdir, "log.txt")
        with open(tmp_log, "w", encoding="utf-8") as f:
            f.write(txt)
        upload_file(svc, logs_id, tmp_log, logname, "text/plain")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == "__main__":
    run()

# scripts/renderer.py
import os, io, json, random, tempfile, subprocess
from datetime import datetime, timezone
from gtts import gTTS
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

# ---- ENV ----
CLIENT_ID = os.environ['OAUTH_CLIENT_ID']
CLIENT_SECRET = os.environ['OAUTH_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['OAUTH_REFRESH_TOKEN']
ROOT_ID = os.environ.get('DRIVE_ROOT_FOLDER_ID')  # usamos o ID direto

SCOPES = [
  'https://www.googleapis.com/auth/drive.file',
  'https://www.googleapis.com/auth/drive.readonly',
  'https://www.googleapis.com/auth/drive.metadata.readonly'
]

def drive():
  creds = Credentials(
      None, refresh_token=REFRESH_TOKEN, token_uri='https://oauth2.googleapis.com/token',
      client_id=CLIENT_ID, client_secret=CLIENT_SECRET, scopes=SCOPES
  )
  return build('drive','v3',credentials=creds, cache_discovery=False)

def ensure_folder(svc, parent_id, name):
  q = f"'{parent_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{name}'"
  r = svc.files().list(q=q, fields="files(id,name)").execute().get('files',[])
  if r: return r[0]['id']
  meta = {'name':name,'mimeType':'application/vnd.google-apps.folder','parents':[parent_id]}
  return svc.files().create(body=meta, fields="id").execute()['id']

def list_files(svc, parent_id, name_contains=None):
  q = [f"'{parent_id}' in parents","trashed=false"]
  if name_contains: q.append(f"name contains '{name_contains}'")
  r = svc.files().list(q=" and ".join(q), orderBy="modifiedTime desc",
                       fields="files(id,name,mimeType,modifiedTime,size)").execute()
  return r.get('files',[])

def download_text(svc, file_id):
  req = svc.files().get_media(fileId=file_id)
  buf = io.BytesIO()
  MediaIoBaseDownload(buf, req).next_chunk()
  return buf.getvalue().decode('utf-8','ignore')

def download_bytes(svc, file_id):
  req = svc.files().get_media(fileId=file_id)
  buf = io.BytesIO(); done=False
  d = MediaIoBaseDownload(buf, req)
  while not done:
    _, done = d.next_chunk()
  return buf.getvalue()

def upload_binary(svc, parent_id, name, data_bytes, mime):
  media = MediaIoBaseUpload(io.BytesIO(data_bytes), mimetype=mime, resumable=False)
  meta = {'name': name, 'parents':[parent_id]}
  return svc.files().create(body=meta, media_body=media, fields="id,name").execute()

def latest_work_orders(svc, cfg_id):
  files = list_files(svc, cfg_id, "work_orders_")
  return files[0] if files else None

def pick_music(svc, mus_bg_id, mus_ave_id, policy, faixa):
  if policy=='ave_maria':
    if faixa:
      r = svc.files().list(q=f"'{mus_ave_id}' in parents and trashed=false and name='{faixa}'",
                           fields="files(id,name)").execute().get('files',[])
      if r: return r[0]
    r = list_files(svc, mus_ave_id)
    if not r: raise SystemExit("Ave Maria não encontrada em 01_assets_musicas_ave_maria")
    return r[0]
  r = list_files(svc, mus_bg_id)
  if not r: raise SystemExit("BG músicas vazia em 01_assets_musicas")
  random.shuffle(r); return r[0]

def tsv_for_slot(svc, scripts_id, slot):
  files = list_files(svc, scripts_id, f"run_{slot}.tsv")
  return download_text(svc, files[0]['id']) if files else None

def synth_tts(text, lang):
  gtts_lang = {'pt':'pt','en':'en','es':'es','pl':'pl'}.get(lang,'pt')
  tts = gTTS(text=text, lang=gtts_lang)
  buf = io.BytesIO(); tts.write_to_fp(buf)
  return buf.getvalue()

def fix_image_1080(src_bytes):
  # força 1920x1080 com letterbox
  in_tmp = tempfile.NamedTemporaryFile(delete=False).name
  out_tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.jpg').name
  with open(in_tmp,'wb') as f: f.write(src_bytes)
  subprocess.check_call([
    'ffmpeg','-y','-i',in_tmp,
    '-vf','scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2',
    '-q:v','2', out_tmp
  ])
  return out_tmp

def make_video(img_paths, tts_wav, music_wav, out_path):
  # 40 imagens x 12s = 480s (8 min). Ajusta se tiver menos imagens.
  dur = max(480 // max(1,len(img_paths)), 8)
  concat_txt = "\n".join([f"file '{p}'\nduration {dur}" for p in img_paths]) + f"\nfile '{img_paths[-1]}'\n"
  listfile = tempfile.NamedTemporaryFile('w', delete=False, suffix='.txt').name
  with open(listfile,'w') as f: f.write(concat_txt)

  subprocess.check_call([
    'ffmpeg','-y',
    '-f','concat','-safe','0','-i', listfile,
    '-i', tts_wav, '-i', music_wav,
    '-filter_complex',
    "[0:v]scale=1920:1080,format=yuv420p[v];"
    "[2:a]volume=0.15[bg];"
    "[1:a][bg]sidechaincompress=threshold=0.02:ratio=8:attack=20:release=200[mix]",
    '-map','[v]','-map','[mix]',
    '-r','25','-c:v','libx264','-pix_fmt','yuv420p','-preset','medium','-crf','18',
    '-shortest', out_path
  ])

def run():
  svc = drive()
  # Pastas base
  cfg_id     = ensure_folder(svc, ROOT_ID, '00_config')
  scripts_id = ensure_folder(svc, ROOT_ID, '02_scripts_autogerados')
  out_pt     = ensure_folder(svc, ROOT_ID, '03_outputs_videos_pt')
  out_en     = ensure_folder(svc, ROOT_ID, '03_outputs_videos_en')
  out_es     = ensure_folder(svc, ROOT_ID, '03_outputs_videos_es')
  out_pl     = ensure_folder(svc, ROOT_ID, '03_outputs_videos_pl')
  mus_bg_id  = ensure_folder(svc, ROOT_ID, '01_assets_musicas')
  mus_ave_id = ensure_folder(svc, ROOT_ID, '01_assets_musicas_ave_maria')
  img_jesus  = ensure_folder(svc, ROOT_ID, '01_assets_imagens_jesus')
  img_maria  = ensure_folder(svc, ROOT_ID, '01_assets_imagens_maria')

  wo = latest_work_orders(svc, cfg_id)
  if not wo:
    print("Sem work_orders*.json em 00_config.")
    return
  jobs = json.loads(download_text(svc, wo['id']))
  if not jobs:
    print("JSON vazio.")
    return

  for job in jobs:
    idioma = job['idioma']
    slot = job['slot']
    personagem = 'maria' if slot.startswith('maria') else 'jesus'
    out_folder = {'pt':out_pt,'en':out_en,'es':out_es,'pl':out_pl}.get(idioma, out_pt)

    # TSV → texto TTS
    tsv_txt = tsv_for_slot(svc, scripts_id, slot)
    if not tsv_txt:
      print(f"TSV ausente para {slot}. Pulando.")
      continue
    lines = [l.split('\t') for l in tsv_txt.strip().split('\n') if l.strip()]
    phrases = [c[3] for c in lines if len(c)>=4]
    tts_text = " ".join(phrases)

    # TTS (mp3 → wav 48k)
    tts_mp3 = synth_tts(tts_text, idioma)
    tts_mp3_path = tempfile.NamedTemporaryFile(delete=False, suffix='.mp3').name
    with open(tts_mp3_path,'wb') as f: f.write(tts_mp3)
    tts_wav_path = tempfile.NamedTemporaryFile(delete=False, suffix='.wav').name
    subprocess.check_call(['ffmpeg','-y','-i',tts_mp3_path,'-ar','48000','-ac','2',tts_wav_path])

    # Música
    mus_file = pick_music(svc, mus_bg_id, mus_ave_id, job.get('policy','bg_random'), job.get('faixa_ave_maria',''))
    mus_src = tempfile.NamedTemporaryFile(delete=False).name
    with open(mus_src,'wb') as f: f.write(download_bytes(svc, mus_file['id']))
    music_wav = tempfile.NamedTemporaryFile(delete=False, suffix='.wav').name
    subprocess.check_call(['ffmpeg','-y','-i',mus_src,'-ar','48000','-ac','2',music_wav])

    # Imagens (40)
    imgs_parent = img_maria if personagem=='maria' else img_jesus
    imgs = [f for f in list_files(svc, imgs_parent) if f['mimeType'].startswith('image/')]
    if not imgs:
      print(f"Sem imagens para {personagem}.")
      continue
    random.shuffle(imgs)
    imgs = imgs[:40] if len(imgs)>=40 else imgs
    fixed_paths = [fix_image_1080(download_bytes(svc, f['id'])) for f in imgs]

    # Render
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    out_name = f"{slot}_{idioma}_{ts}.mp4"
    out_file = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4').name
    make_video(fixed_paths, tts_wav_path, music_wav, out_file)

    # Upload MP4
    with open(out_file,'rb') as fh:
      upload_binary(svc, out_folder, out_name, fh.read(), 'video/mp4')
    print(f"OK: {out_name}")

if __name__ == '__main__':
  run()

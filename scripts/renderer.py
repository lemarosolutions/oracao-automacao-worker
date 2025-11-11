# scripts/renderer.py
import os, io, random, json, time, tempfile, re, math, shutil, glob, string
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request
from gtts import gTTS
from PIL import Image, ImageDraw, ImageFont
import subprocess as sp

# -------------------- CONFIG --------------------
TARGET_SEC = 480
FPS = 30
W, H = 1920, 1080
ZOOM = 1.08          # leve Ken Burns
XFAD = 0.6           # crossfade entre imagens
SAY_TYPES = {'exame','suplica','verso','intercessao','agradecimento','encerramento','cta','texto','mensagem'}

# -------------------- OAUTH ---------------------
def svc_from_oauth():
    client_id     = os.getenv('OAUTH_CLIENT_ID')
    client_secret = os.getenv('OAUTH_CLIENT_SECRET')
    refresh_token = os.getenv('OAUTH_REFRESH_TOKEN')
    token_uri = 'https://oauth2.googleapis.com/token'
    creds = Credentials(None,
                        refresh_token=refresh_token,
                        token_uri=token_uri,
                        client_id=client_id,
                        client_secret=client_secret,
                        scopes=[
                            'https://www.googleapis.com/auth/drive.file',
                            'https://www.googleapis.com/auth/drive.metadata.readonly',
                            'https://www.googleapis.com/auth/spreadsheets.readonly'
                        ])
    creds.refresh(Request())
    return build('drive','v3',credentials=creds)

# -------------------- DRIVE HELPERS -------------
def list_by_name(svc, parent_id, name):
    q = f"'{parent_id}' in parents and trashed=false and name='{name}'"
    r = svc.files().list(q=q,fields="files(id,name)").execute()
    return r.get('files',[])

def ensure_folder(svc, parent_id, name):
    r = list_by_name(svc,parent_id,name)
    if r: return r[0]['id']
    meta={'name':name,'mimeType':'application/vnd.google-apps.folder','parents':[parent_id]}
    return svc.files().create(body=meta,fields='id').execute()['id']

def download_text(svc, file_id):
    req = svc.files().get_media(fileId=file_id)
    buf = io.BytesIO(); downloader = MediaIoBaseDownload(buf, req)
    done=False
    while not done:
        status, done = downloader.next_chunk()
    return buf.getvalue().decode('utf-8')

def upload_file(svc, parent_id, local_path, name, mime):
    meta={'name':name,'parents':[parent_id]}
    media=MediaIoBaseUpload(open(local_path,'rb'), mimetype=mime, resumable=True)
    return svc.files().create(body=meta, media_body=media, fields='id').execute()['id']

def pick_any(svc, folder_id, exts):
    q = f"'{folder_id}' in parents and trashed=false"
    r = svc.files().list(q=q,fields="files(id,name,mimeType,size)",pageSize=1000).execute()
    files=r.get('files',[])
    cand=[f for f in files if any(f['name'].lower().endswith(e) for e in exts)]
    if not cand: return None
    f=random.choice(cand)
    # download to tmp
    req = svc.files().get_media(fileId=f['id'])
    fd,tmp = tempfile.mkstemp(suffix='_'+f['name'])
    os.close(fd)
    with open(tmp,'wb') as out:
        downloader = MediaIoBaseDownload(out, req)
        done=False
        while not done:
            status, done = downloader.next_chunk()
    return tmp

def list_all_local(svc, folder_id, exts, limit=50):
    q = f"'{folder_id}' in parents and trashed=false"
    r = svc.files().list(q=q,fields="files(id,name)",pageSize=1000).execute()
    ids=[(x['id'],x['name']) for x in r.get('files',[]) if any(x['name'].lower().endswith(e) for e in exts)]
    random.shuffle(ids)
    ids=ids[:limit] if limit else ids
    paths=[]
    for fid,name in ids:
        req=svc.files().get_media(fileId=fid)
        fd,tmp=tempfile.mkstemp(suffix='_'+name); os.close(fd)
        with open(tmp,'wb') as out:
            dl=MediaIoBaseDownload(out,req); done=False
            while not done:
                status,done=dl.next_chunk()
        paths.append(tmp)
    return paths

# -------------------- MEDIA HELPERS -------------
def run(cmd):
    cp=sp.run(cmd, shell=True, stdout=sp.PIPE, stderr=sp.STDOUT, text=True)
    if cp.returncode!=0: raise RuntimeError(cp.stdout)
    return cp.stdout

def seconds_of(audio_path):
    out=run(f'ffprobe -v error -show_entries format=duration -of default=nw=1:nk=1 "{audio_path}"')
    return float(out.strip())

def build_tts(text, out_wav):
    # gTTS -> mp3 -> wav; grave (pitch down) ~10%
    tmp_mp3 = out_wav.replace('.wav','.mp3')
    gTTS(text=text, lang='pt', slow=False).save(tmp_mp3)
    # pitch down: asetrate + atempo (aprox -2 semitons)
    run(f'ffmpeg -y -i "{tmp_mp3}" -filter:a "asetrate=44100*0.89,aresample=44100,atempo=1.12" "{out_wav}"')
    os.remove(tmp_mp3)

def build_slideshow(imgs, dur, out_mp4):
    if len(imgs)<2:
        # duplica se necessário
        if imgs: imgs = imgs*2
        else: raise RuntimeError('Sem imagens para slideshow')
    per = max(3.0, (dur - (len(imgs)-1)*XFAD) / len(imgs))
    inputs=' '.join(f'-loop 1 -t {per+XFAD:.3f} -i "{p}"' for p in imgs)
    zoom = f"scale={W}:{H},zoompan=z='min(max(pzoom,{ZOOM})':d=1*{FPS}:s={W}x{H}"
    xfade_chain=[]
    for i in range(len(imgs)-1):
        a = f'v{i}' if i>0 else '0:v'
        b = f'{i+1}:v'
        out = f'v{i+1}'
        xfade_chain.append(f'[{a}][{b}]xfade=transition=crossfade:duration={XFAD}:offset={(i+1)*per + i*XFAD:.3f}[{out}]')
    # filtros por entrada
    prep = ';'.join([f'[{i}:v]{zoom},format=yuv420p[v{i}]' for i in range(len(imgs))])
    chain = ';'.join([prep]+xfade_chain)
    final = f'-map [v{len(imgs)-1}] -r {FPS} -pix_fmt yuv420p -movflags +faststart -vf scale={W}:{H}'
    cmd = f'ffmpeg -y {inputs} -filter_complex "{chain}" -t {dur:.3f} {final} "{out_mp4}"'
    run(cmd)

def mix_av(narr_wav, music_wav, target_sec, out_wav):
    # loop música e mix baixo (-18dB)
    run(f'ffmpeg -y -stream_loop -1 -i "{music_wav}" -i "{narr_wav}" '
        f'-filter_complex "[0:a]volume=0.18,aloop=loop=-1:size=2e6[a0];'
        f'[a0]atrim=0:{target_sec}[m];[1:a]atrim=0:{target_sec}[v];'
        f'[v][m]amix=inputs=2:normalize=0[a]" -map "[a]" -t {target_sec} "{out_wav}"')

def make_thumb(base_img, title, out_png):
    img = Image.open(base_img).convert('RGB').resize((W,H))
    draw = ImageDraw.Draw(img)
    # caixa preta translúcida
    overlay = Image.new('RGBA', img.size, (0,0,0,140))
    img = Image.alpha_composite(img.convert('RGBA'), overlay)
    # texto
    fsize = 88
    try:
        font = ImageFont.truetype("DejaVuSans-Bold.ttf", fsize)
    except:
        font = ImageFont.load_default()
    lines=[]
    words=title.strip()[:70].split()
    line=""
    for w in words:
        if len(line+' '+w) < 22: line = (line+' '+w).strip()
        else: lines.append(line); line=w
    if line: lines.append(line)
    y = H//2 - (len(lines)*fsize)//2
    for ln in lines:
        wtxt,htxt = draw.textsize(ln, font=font)
        x = (W-wtxt)//2
        draw.text((x,y), ln, font=font, fill=(255,255,255,240))
        y += int(fsize*1.15)
    img.convert('RGB').save(out_png, 'JPEG', quality=92)

# -------------------- TSV PARSE -----------------
def clean_line(s):
    s = re.sub(r'\[.*?\]','', str(s))  # remove [VIDEO]/[VOZ] etc
    return s.strip()

def load_tsv(path):
    rows=[]
    with open(path,'r',encoding='utf-8') as f:
        for ln in f:
            parts=ln.rstrip('\n').split('\t')
            if len(parts)>=4:
                rows.append({'run':parts[0],'ord':int(parts[1]),'tipo':parts[2].lower().strip(),'txt':parts[3]})
    rows.sort(key=lambda x:x['ord'])
    return rows

def narration_from_rows(rows):
    texts=[]
    policy='bg_random'
    for r in rows:
        t=r['tipo']
        if t=='musica_policy':
            policy = clean_line(r['txt']).lower()
            continue
        if t in SAY_TYPES:
            val=clean_line(r['txt'])
            if val: texts.append(val)
    base=' '.join(texts)
    # est. 130 wpm ~ 2.16 wps -> 480s ~ 1036 palavras; se pouco, repete suavemente
    words=base.split()
    if len(words)<700:
        rep = math.ceil(900/ max(1,len(words)))
        base = (' ' + (' '.join(texts) + ' ')).join(['']*rep).strip()
    return base, policy

# -------------------- MAIN ----------------------
def run():
    svc = svc_from_oauth()
    ROOT = os.getenv('DRIVE_ROOT_FOLDER_ID')
    # pastas
    cfg_id  = ensure_folder(svc, ROOT, '00_config')
    out_pt  = ensure_folder(svc, ROOT, '03_outputs_videos_pt')
    out_en  = ensure_folder(svc, ROOT, '03_outputs_videos_en')
    out_es  = ensure_folder(svc, ROOT, '03_outputs_videos_es')
    out_pl  = ensure_folder(svc, ROOT, '03_outputs_videos_pl')
    th_pt   = ensure_folder(svc, ROOT, '04_outputs_thumbnails_pt')
    th_en   = ensure_folder(svc, ROOT, '04_outputs_thumbnails_en')
    th_es   = ensure_folder(svc, ROOT, '04_outputs_thumbnails_es')
    th_pl   = ensure_folder(svc, ROOT, '04_outputs_thumbnails_pl')
    img_j   = ensure_folder(svc, ROOT, '01_assets_imagens_jesus')
    img_m   = ensure_folder(svc, ROOT, '01_assets_imagens_maria')
    brolls  = ensure_folder(svc, ROOT, '01_assets_brolls')
    mus_bg  = ensure_folder(svc, ROOT, '01_assets_musicas')
    mus_am  = ensure_folder(svc, ROOT, '01_assets_musicas_ave_maria')

    # último work_order
    q = f"'{cfg_id}' in parents and trashed=false and name contains 'work_orders_'"
    r = svc.files().list(q=q, orderBy='modifiedTime desc', pageSize=1,
                         fields="files(id,name)").execute()
    if not r.get('files'): 
        raise RuntimeError('Nenhum work_orders_*.json encontrado em 00_config.')
    wo_id = r['files'][0]['id']
    work = json.loads(download_text(svc, wo_id))

    tmpdir = tempfile.mkdtemp()
    try:
        for job in work:
            lang = job['idioma']
            slot = job['slot']
            title= job['title']
            policy = job.get('policy','bg_random').lower()

            # pega TSV run_<slot>.tsv da pasta 02_scripts_autogerados
            scripts_id = ensure_folder(svc, ROOT, '02_scripts_autogerados')
            tsq = f"'{scripts_id}' in parents and trashed=false and name='run_{slot}.tsv'"
            tr = svc.files().list(q=tsq, fields="files(id,name)").execute()
            if not tr.get('files'): 
                # se não existe, pula job
                continue
            tsv_local = os.path.join(tmpdir, f'run_{slot}.tsv')
            req = svc.files().get_media(fileId=tr['files'][0]['id'])
            with open(tsv_local,'wb') as out:
                dl = MediaIoBaseDownload(out, req); done=False
                while not done:
                    status,done=dl.next_chunk()

            rows = load_tsv(tsv_local)
            narr_txt, pol = narration_from_rows(rows)
            if policy=='bg_random': policy = pol

            # TTS
            narr_wav = os.path.join(tmpdir,'narr.wav')
            build_tts(narr_txt, narr_wav)
            narr_len = seconds_of(narr_wav)

            # imagens
            base_imgs = list_all_local(svc, img_m if 'maria' in slot else img_j, ('.jpg','.jpeg','.png'), limit=20)
            if len(base_imgs)<2:
                extra = list_all_local(svc, brolls, ('.jpg','.jpeg','.png'), limit=10)
                base_imgs += extra
            # vídeo (slideshow) no tamanho da narração, mas garantimos 480s ao final
            vid_mp4 = os.path.join(tmpdir,'vid.mp4')
            base_dur = min(max(narr_len, 60.0), TARGET_SEC)  # pelo menos 60s para efeito
            build_slideshow(base_imgs, base_dur, vid_mp4)

            # música
            music_src = pick_any(svc, mus_am if (slot=='maria_v2' and policy=='ave_maria') else mus_bg, ('.mp3','.wav','.m4a'))
            if not music_src:
                # sem música, duplica apenas a voz no tempo alvo
                bg_mix = os.path.join(tmpdir,'mix.wav')
                run(f'ffmpeg -y -i "{narr_wav}" -t {TARGET_SEC} -af "apad=pad_dur={TARGET_SEC}" "{bg_mix}"')
            else:
                bg_mix = os.path.join(tmpdir,'mix.wav')
                mix_av(narr_wav, music_src, TARGET_SEC, bg_mix)

            # mux final (corta/estende vídeo para 480s)
            final_mp4 = os.path.join(tmpdir, f'{slot}_{lang}_{datetime.utcnow().strftime("%Y%m%d")}.mp4')
            run(f'ffmpeg -y -stream_loop -1 -i "{vid_mp4}" -i "{bg_mix}" -shortest -t {TARGET_SEC} '
                f'-map 0:v:0 -map 1:a:0 -c:v libx264 -preset veryfast -crf 20 -c:a aac -b:a 160k -pix_fmt yuv420p "{final_mp4}"')

            # thumbnail
            thumb_jpg = os.path.join(tmpdir, 'thumb.jpg')
            make_thumb(base_imgs[0], title, thumb_jpg)

            # upload
            out_folder = {'pt':out_pt,'en':out_en,'es':out_es,'pl':out_pl}[lang]
            th_folder  = {'pt':th_pt,'en':th_en,'es':th_es,'pl':th_pl}[lang]
            upload_file(svc, out_folder, final_mp4, os.path.basename(final_mp4), 'video/mp4')
            upload_file(svc, th_folder,  thumb_jpg, f'thumb_{slot}_{datetime.utcnow().strftime("%Y%m%d")}.jpg', 'image/jpeg')

        # log curtinho no Drive/05_logs
        logs_id = ensure_folder(svc, ROOT, '05_logs')
        logname = f'log_renderer_{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.txt'
        txt = f'UTC:{datetime.utcnow().isoformat()} status:OK steps:render_v2 completed'
        tmp = os.path.join(tmpdir, 'log.txt'); open(tmp,'w',encoding='utf-8').write(txt)
        upload_file(svc, logs_id, tmp, logname, 'text/plain')

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

if __name__ == '__main__':
    run()

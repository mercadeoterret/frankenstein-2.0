"""
TÉRRET · Core — Lógica de negocio sin dependencias de UI
Reutilizable desde FastAPI, CLI, o cualquier otro framework
"""

import os
import io
import time
import subprocess
import pandas as pd
import requests
from google.oauth2.service_account import Credentials

# ── Configuración global ────────────────────────────────
GOOGLE_SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/19ae5_2AFNAc8V_CvbuaXl_MWI4OON5EiwJEWDUPiBTM/export?format=csv&gid=0"
SHEET_ID             = "19ae5_2AFNAc8V_CvbuaXl_MWI4OON5EiwJEWDUPiBTM"
MASTER_PASSWORD      = "terret2024"
DRIVE_ROOT_FOLDER_ID = "0AB2EtnClHpnrUk9PVA"
SHARED_DRIVE_ID      = "0AB2EtnClHpnrUk9PVA"
META_ACCESS_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "")
META_AD_ACCOUNT_ID   = os.environ.get("META_AD_ACCOUNT_ID", "")

ESTADOS = ["Pendiente", "Listo", "Publicado", "Pausado"]
ESTADO_CONFIG = {
    "Pendiente": {"icon": "—",  "color": "#71717A", "bg": "#F9F9F9", "border": "#E4E4E7"},
    "Listo":     {"icon": "✓",  "color": "#16A34A", "bg": "#F0FDF4", "border": "#BBF7D0"},
    "Publicado": {"icon": "↑",  "color": "#09090B", "bg": "#F4F4F5", "border": "#D4D4D8"},
    "Pausado":   {"icon": "∥",  "color": "#71717A", "bg": "#F9F9F9", "border": "#E4E4E7"},
}

# ── Credenciales ────────────────────────────────────────
def obtener_credenciales():
    import json
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    # 1. Archivo local (desarrollo)
    if os.path.exists("credenciales.json"):
        return Credentials.from_service_account_file("credenciales.json", scopes=scopes)
    # 2. Secret File de Render
    if os.path.exists("/etc/secrets/service_account.json"):
        return Credentials.from_service_account_file("/etc/secrets/service_account.json", scopes=scopes)
    # 3. Variable de entorno JSON completo (Render Environment Variables)
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if sa_json:
        info = json.loads(sa_json)
        return Credentials.from_service_account_info(info, scopes=scopes)
    # 4. Variables individuales (legacy)
    key = os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n")
    if key:
        info = {
            "type": "service_account",
            "project_id":                os.environ.get("GCP_PROJECT_ID", ""),
            "private_key_id":            os.environ.get("GCP_PRIVATE_KEY_ID", ""),
            "private_key":               key,
            "client_email":              os.environ.get("GCP_CLIENT_EMAIL", ""),
            "client_id":                 os.environ.get("GCP_CLIENT_ID", ""),
            "auth_uri":                  "https://accounts.google.com/o/oauth2/auth",
            "token_uri":                 "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url":      os.environ.get("GCP_CLIENT_CERT_URL", ""),
            "universe_domain":           "googleapis.com",
        }
        return Credentials.from_service_account_info(info, scopes=scopes)
    return None

def get_drive_service():
    from googleapiclient.discovery import build
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("Sin credenciales de Google")
    return build("drive", "v3", credentials=creds)

def get_gspread_client():
    import gspread
    creds = obtener_credenciales()
    if not creds:
        raise ValueError("Sin credenciales de Google")
    return gspread.authorize(creds)

# ── Helpers ─────────────────────────────────────────────
def extraer_creador(nombre):
    partes = str(nombre).split("_")
    return partes[1] if len(partes) > 1 else "Desconocido"

def extraer_genero(nombre):
    partes = str(nombre).split("_")
    if len(partes) > 2:
        g = partes[2].upper()
        return g if g in ["M", "F"] else "N/A"
    return "N/A"

def extract_file_id(url):
    if not url or str(url).strip().lower() in ["", "nan"]:
        return None
    url = str(url).strip()
    if "drive.google.com/file/d/" in url:
        try:
            return url.split("/file/d/")[1].split("/")[0]
        except Exception:
            return None
    return None

def _to_num(val):
    """Convierte string con coma decimal (locale colombiano) a float."""
    try:
        return float(str(val).replace(",", ".").replace("\xa0", "").strip())
    except Exception:
        return 0.0

# ── Google Sheets — Lecturas ────────────────────────────
def load_sheet() -> pd.DataFrame:
    try:
        r = requests.get(f"{GOOGLE_SHEET_CSV_URL}&_t={int(time.time())}", timeout=10)
        r.encoding = "utf-8"
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        if "caracteristica" not in df.columns:
            df["caracteristica"] = ""
        return df
    except Exception:
        return pd.DataFrame()

def load_active_ads() -> pd.DataFrame:
    try:
        client = get_gspread_client()
        sheet  = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        records = sheet.get_all_records(value_render_option="UNFORMATTED_VALUE")
        df = pd.DataFrame(records)
        for col in ["inversion", "roas", "compras", "ctr", "hook_rate",
                    "hold_rate", "cpm", "cpa", "impresiones"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
            else:
                df[col] = 0.0
        if "estado" not in df.columns:
            df["estado"] = "Pendiente"
        df["estado"] = df["estado"].fillna("Pendiente")
        df["ingresos"] = df["inversion"] * df["roas"]
        df["creador"]  = df["hook"].apply(extraer_creador) if "hook" in df.columns else ""
        return df
    except Exception:
        return pd.DataFrame()

def load_briefs() -> pd.DataFrame:
    try:
        client = get_gspread_client()
        return pd.DataFrame(client.open_by_key(SHEET_ID).worksheet("Briefs").get_all_records())
    except Exception:
        return pd.DataFrame()

def load_voice_overs() -> pd.DataFrame:
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("VoiceOvers")
        except Exception:
            ws = sh.add_worksheet("VoiceOvers", rows=500, cols=8)
            ws.append_row(["producto","nombre","audio_url","descripcion",
                           "locutor","duracion_seg","idioma","fecha"])
        df = pd.DataFrame(ws.get_all_records())
        return df if not df.empty else pd.DataFrame(
            columns=["producto","nombre","audio_url","descripcion",
                     "locutor","duracion_seg","idioma","fecha"])
    except Exception:
        return pd.DataFrame()

def load_caracteristicas() -> pd.DataFrame:
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("Caracteristicas")
        except Exception:
            ws = sh.add_worksheet("Caracteristicas", rows=500, cols=3)
            ws.append_row(["producto", "caracteristica", "descripcion"])
        df = pd.DataFrame(ws.get_all_records())
        return df if not df.empty else pd.DataFrame(
            columns=["producto", "caracteristica", "descripcion"])
    except Exception:
        return pd.DataFrame()

def get_chars_producto(producto, df_all=None, df_chars=None):
    chars = set()
    if df_chars is not None and not df_chars.empty and "producto" in df_chars.columns:
        chars.update(df_chars[df_chars["producto"] == producto]["caracteristica"].dropna().tolist())
    if df_all is not None and not df_all.empty:
        bodies = df_all[(df_all["producto"] == producto) & (df_all["tipo"] == "Body")]
        if "caracteristica" in bodies.columns:
            chars.update(bodies["caracteristica"].dropna().tolist())
    return sorted([c for c in chars if str(c).strip() and str(c).lower() != "nan"])

# ── Google Sheets — Escrituras ──────────────────────────
def append_to_sheet(row_data: list) -> bool:
    try:
        client = get_gspread_client()
        client.open_by_key(SHEET_ID).sheet1.append_row(
            [str(i) if i else "" for i in row_data],
            value_input_option="USER_ENTERED"
        )
        return True
    except Exception:
        return False

def guardar_anuncio(row_data: list) -> bool:
    try:
        client = get_gspread_client()
        client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos").append_row(
            [str(i) if i else "" for i in row_data],
            value_input_option="USER_ENTERED"
        )
        return True
    except Exception:
        return False

def actualizar_metricas_anuncio(nombre, inversion, compras, ctr, roas,
                                 hook_rate=0, hold_rate=0, cpa=0) -> bool:
    try:
        client = get_gspread_client()
        sheet  = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        cell   = sheet.find(nombre, in_column=1)
        if cell:
            sheet.batch_update([
                {"range": f"H{cell.row}", "values": [[inversion]]},
                {"range": f"I{cell.row}", "values": [[compras]]},
                {"range": f"J{cell.row}", "values": [[ctr]]},
                {"range": f"K{cell.row}", "values": [[roas]]},
                {"range": f"O{cell.row}", "values": [[hook_rate]]},
                {"range": f"P{cell.row}", "values": [[hold_rate]]},
                {"range": f"S{cell.row}", "values": [[cpa]]},
            ])
            return True
        return False
    except Exception:
        return False

def actualizar_estado_anuncio(nombre, nuevo_estado, video_url="", razon="") -> bool:
    try:
        client = get_gspread_client()
        sheet  = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        cell   = sheet.find(nombre, in_column=1)
        if cell:
            sheet.update_cell(cell.row, 12, nuevo_estado)
            if video_url: sheet.update_cell(cell.row, 13, video_url)
            if razon:     sheet.update_cell(cell.row, 14, razon)
            return True
        return False
    except Exception:
        return False

def eliminar_anuncio(nombre) -> bool:
    try:
        client = get_gspread_client()
        sheet  = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        cell   = sheet.find(nombre, in_column=1)
        if cell:
            sheet.delete_rows(cell.row)
            return True
        return False
    except Exception:
        return False

def guardar_o_actualizar_brief(producto, hooks, bodies, ctas, guiones) -> bool:
    try:
        client = get_gspread_client()
        sh     = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("Briefs")
        except Exception:
            ws = sh.add_worksheet("Briefs", rows=200, cols=6)
            ws.append_row(["producto","hooks","bodies","ctas","guiones","fecha"])
        cell = ws.find(producto, in_column=1)
        row  = [producto, hooks, bodies, ctas, guiones,
                time.strftime("%Y-%m-%d")]
        if cell:
            ws.update(f"A{cell.row}:F{cell.row}", [row])
        else:
            ws.append_row(row)
        return True
    except Exception:
        return False

def eliminar_asset(nombre) -> bool:
    try:
        client = get_gspread_client()
        sheet  = client.open_by_key(SHEET_ID).sheet1
        cell   = sheet.find(nombre, in_column=3)
        if cell:
            sheet.delete_rows(cell.row)
            return True
        return False
    except Exception:
        return False

def guardar_caracteristica(producto, caracteristica) -> bool:
    try:
        client = get_gspread_client()
        sh = client.open_by_key(SHEET_ID)
        try:
            ws = sh.worksheet("Caracteristicas")
        except Exception:
            ws = sh.add_worksheet("Caracteristicas", rows=500, cols=3)
            ws.append_row(["producto", "caracteristica", "descripcion"])
        ws.append_row([producto, caracteristica, ""])
        return True
    except Exception:
        return False

def eliminar_caracteristica(producto, caracteristica) -> bool:
    try:
        client = get_gspread_client()
        ws = client.open_by_key(SHEET_ID).worksheet("Caracteristicas")
        records = ws.get_all_records()
        for i, r in enumerate(records, start=2):
            if r.get("producto") == producto and r.get("caracteristica") == caracteristica:
                ws.delete_rows(i)
                return True
        return False
    except Exception:
        return False

# ── Google Drive ────────────────────────────────────────
def get_or_create_folder(service, name, parent_id):
    q = (f"name='{name}' and mimeType='application/vnd.google-apps.folder' "
         f"and '{parent_id}' in parents and trashed=false")
    res = service.files().list(
        q=q, fields="files(id)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]
    return service.files().create(
        body={"name": name,
              "mimeType": "application/vnd.google-apps.folder",
              "parents": [parent_id]},
        fields="id", supportsAllDrives=True
    ).execute()["id"]

def borrar_archivo_drive(service, file_id) -> bool:
    try:
        service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
        return True
    except Exception:
        return False

def download_file_from_drive(service, file_id, output_path):
    from googleapiclient.http import MediaIoBaseDownload
    req = service.files().get_media(fileId=file_id)
    with open(output_path, "wb") as f:
        dl = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = dl.next_chunk()

def upload_video_to_drive(file_bytes, filename, producto, tipo) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    service     = get_drive_service()
    assets_id   = get_or_create_folder(service, "Assets", DRIVE_ROOT_FOLDER_ID)
    prod_id     = get_or_create_folder(service, producto, assets_id)
    tipo_id     = get_or_create_folder(service, tipo + "s", prod_id)

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "mp4"
    mime_map = {"mp4": "video/mp4", "mov": "video/quicktime",
                "avi": "video/x-msvideo", "mkv": "video/x-matroska"}
    mimetype = mime_map.get(ext, "video/mp4")

    uploaded = service.files().create(
        body={"name": filename, "parents": [tipo_id]},
        media_body=MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mimetype, resumable=True),
        fields="id", supportsAllDrives=True
    ).execute()
    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True
    ).execute()
    fid = uploaded["id"]
    # Wake-up para acelerar procesamiento de Drive
    for url in [f"https://drive.google.com/thumbnail?id={fid}&sz=w400",
                f"https://drive.google.com/file/d/{fid}/preview"]:
        try:
            requests.get(url, timeout=8)
        except Exception:
            pass
    return f"https://drive.google.com/file/d/{fid}/view"

def upload_final_to_drive(file_bytes, filename, producto) -> str:
    from googleapiclient.http import MediaIoBaseUpload
    service    = get_drive_service()
    finales_id = get_or_create_folder(service, "Anuncios_Finales", DRIVE_ROOT_FOLDER_ID)
    prod_id    = get_or_create_folder(service, producto, finales_id)
    uploaded   = service.files().create(
        body={"name": filename, "parents": [prod_id]},
        media_body=MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="video/mp4", resumable=True),
        fields="id", supportsAllDrives=True
    ).execute()
    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True
    ).execute()
    return f"https://drive.google.com/file/d/{uploaded['id']}/view"

def upload_vo_to_drive(file_bytes, filename, producto, ext="mp3") -> str:
    from googleapiclient.http import MediaIoBaseUpload
    service  = get_drive_service()
    assets_id = get_or_create_folder(service, "Assets", DRIVE_ROOT_FOLDER_ID)
    prod_id   = get_or_create_folder(service, producto, assets_id)
    vo_id     = get_or_create_folder(service, "VOs", prod_id)
    mime_map  = {"mp3": "audio/mpeg", "wav": "audio/wav",
                 "m4a": "audio/mp4", "ogg": "audio/ogg"}
    mimetype  = mime_map.get(ext.lower(), "audio/mpeg")
    uploaded  = service.files().create(
        body={"name": filename, "parents": [vo_id]},
        media_body=MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mimetype, resumable=True),
        fields="id", supportsAllDrives=True
    ).execute()
    service.permissions().create(
        fileId=uploaded["id"],
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True
    ).execute()
    return f"https://drive.google.com/file/d/{uploaded['id']}/view"

def append_to_sheet_full(row_data: list) -> bool:
    """Registra un asset en la Hoja 1."""
    return append_to_sheet(row_data)

# ── FFmpeg ──────────────────────────────────────────────
def _find_bin(name):
    local = os.path.join(os.path.dirname(__file__), f"{name}")
    if os.path.exists(local):
        return local
    local_exe = local + ".exe"
    if os.path.exists(local_exe):
        return local_exe
    return name

FFMPEG  = _find_bin("ffmpeg")
FFPROBE = _find_bin("ffprobe")

def tiene_audio(filepath) -> bool:
    try:
        cmd = [FFPROBE, "-v", "error", "-select_streams", "a",
               "-show_entries", "stream=codec_type",
               "-of", "default=noprint_wrappers=1:nokey=1", filepath]
        res = subprocess.run(cmd, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
        return "audio" in res.stdout.lower()
    except Exception:
        return True

def convertir_a_mp4_normalizado(input_path, output_path):
    has_audio = tiene_audio(input_path)
    if has_audio:
        cmd = [FFMPEG, "-y", "-i", input_path,
               "-vf", "setpts=PTS-STARTPTS",
               "-af", "asetpts=PTS-STARTPTS",
               "-c:v", "libx264", "-preset", "fast", "-crf", "23",
               "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart", output_path]
    else:
        cmd = [FFMPEG, "-y", "-i", input_path,
               "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
               "-map", "0:v:0", "-map", "1:a:0",
               "-vf", "setpts=PTS-STARTPTS",
               "-c:v", "libx264", "-preset", "fast", "-crf", "23",
               "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart", "-shortest", output_path]
    res = subprocess.run(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True)
    return res.returncode == 0, res.stderr

def concatenar_videos(archivos_mp4, output_path):
    procesados = []
    for i, arc in enumerate(archivos_mp4):
        out = f"conv_{i}.mp4"
        exito, err = convertir_a_mp4_normalizado(arc, out)
        if exito:
            procesados.append(out)
        else:
            for p in procesados:
                if os.path.exists(p): os.remove(p)
            return False, f"Error en clip {i+1}:\n{err[-300:]}"

    with open("concat_list.txt", "w") as f:
        for arc in procesados:
            f.write(f"file '{arc}'\n")

    cmd = [FFMPEG, "-y", "-f", "concat", "-safe", "0", "-i", "concat_list.txt",
           "-c", "copy", "-movflags", "+faststart", output_path]
    result = subprocess.run(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True)

    for p in procesados:
        if os.path.exists(p): os.remove(p)
    if os.path.exists("concat_list.txt"):
        os.remove("concat_list.txt")

    return result.returncode == 0, result.stderr.strip()

# ── Meta API ────────────────────────────────────────────
def obtener_metricas_meta(access_token=None, ad_account_id=None) -> dict:
    token   = access_token  or META_ACCESS_TOKEN
    account = ad_account_id or META_AD_ACCOUNT_ID
    if not token or not account:
        return {"exito": False, "error": "Credenciales de Meta no configuradas"}

    url = f"https://graph.facebook.com/v19.0/{account}/insights"
    params = {
        "access_token": token,
        "level":        "ad",
        "fields": (
            "ad_name,spend,impressions,inline_link_clicks,"
            "actions,action_values,"
            "video_3_sec_watched_actions,"
            "video_p25_watched_actions,"
            "video_p75_watched_actions,"
            "video_p100_watched_actions"
        ),
        "date_preset": "last_30d",
        "limit":       "500",
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        data     = response.json()
        if "error" in data:
            return {"exito": False, "error": data["error"]["message"]}

        resultados = {}
        for ad in data.get("data", []):
            ad_name = ad.get("ad_name", "")
            if not ad_name:
                continue

            spend       = float(ad.get("spend", 0))
            impressions = int(ad.get("impressions", 0))
            clicks      = int(ad.get("inline_link_clicks", 0))
            compras     = 0
            valor       = 0.0

            for action in ad.get("actions", []):
                if action.get("action_type") == "purchase":
                    compras = int(action.get("value", 0))
            for av in ad.get("action_values", []):
                if av.get("action_type") == "purchase":
                    valor = float(av.get("value", 0))

            def _get_video(key):
                for v in ad.get(key, []):
                    if v.get("action_type") == "video_view":
                        return int(v.get("value", 0))
                return 0

            v3s  = _get_video("video_3_sec_watched_actions")
            v25  = _get_video("video_p25_watched_actions")
            v75  = _get_video("video_p75_watched_actions")
            v100 = _get_video("video_p100_watched_actions")

            roas           = round(valor   / spend       * 1, 2) if spend > 0       else 0
            ctr            = round(clicks  / impressions * 100, 2) if impressions > 0 else 0
            hook_rate      = round(v3s     / impressions * 100, 2) if impressions > 0 else 0
            hold_rate      = round(v25     / impressions * 100, 2) if impressions > 0 else 0
            hold_rate_deep = round(v75     / impressions * 100, 2) if impressions > 0 else 0
            view_rate      = round(v100    / impressions * 100, 2) if impressions > 0 else 0
            cpm            = round(spend   / impressions * 1000, 2) if impressions > 0 else 0
            cpa            = round(spend   / compras,      2) if compras > 0        else 0

            resultados[ad_name] = {
                "inversion":       spend,
                "compras":         compras,
                "ctr":             ctr,
                "roas":            roas,
                "hook_rate":       hook_rate,
                "hold_rate":       hold_rate,
                "hold_rate_deep":  hold_rate_deep,
                "view_rate":       view_rate,
                "cpm":             cpm,
                "cpa":             cpa,
                "impresiones":     impressions,
                "clicks":          clicks,
            }

        return {"exito": True, "datos": resultados}
    except Exception as e:
        return {"exito": False, "error": str(e)}

def sincronizar_meta_con_sheets(datos_meta: dict) -> dict:
    """Cruza datos de Meta con Anuncios_Activos y actualiza el Sheet."""
    import unicodedata

    def limpiar(t):
        s = str(t).strip().lower().replace(" ", "").replace("_", "").replace("+", "")
        return "".join(c for c in unicodedata.normalize("NFD", s)
                       if unicodedata.category(c) != "Mn")

    try:
        client    = get_gspread_client()
        sheet     = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        all_rows  = sheet.get_all_values()
        meta_limpio = {limpiar(k): v for k, v in datos_meta.items()}

        actualizados = 0
        no_match     = []

        for row_idx, row in enumerate(all_rows[1:], start=2):
            if not row:
                continue
            sheet_key = limpiar(str(row[0]))
            match_data = None
            for mk, mv in meta_limpio.items():
                if sheet_key in mk or mk in sheet_key:
                    match_data = mv
                    break

            if match_data:
                cvr = round(match_data["compras"] / match_data["clicks"] * 100, 2) \
                      if match_data.get("clicks", 0) > 0 else 0

                def cl(n): return chr(64 + n)
                sheet.batch_update([
                    {"range": f"{cl(8)}{row_idx}",  "values": [[match_data["inversion"]]]},
                    {"range": f"{cl(9)}{row_idx}",  "values": [[match_data["compras"]]]},
                    {"range": f"{cl(10)}{row_idx}", "values": [[match_data["ctr"]]]},
                    {"range": f"{cl(11)}{row_idx}", "values": [[match_data["roas"]]]},
                    {"range": f"{cl(15)}{row_idx}", "values": [[match_data["hook_rate"]]]},
                    {"range": f"{cl(16)}{row_idx}", "values": [[match_data["hold_rate"]]]},
                    {"range": f"{cl(17)}{row_idx}", "values": [[match_data["hold_rate_deep"]]]},
                    {"range": f"{cl(18)}{row_idx}", "values": [[cvr]]},
                    {"range": f"{cl(19)}{row_idx}", "values": [[match_data["cpa"]]]},
                    {"range": f"{cl(20)}{row_idx}", "values": [[match_data["cpm"]]]},
                    {"range": f"{cl(21)}{row_idx}", "values": [[match_data["impresiones"]]]},
                    {"range": f"{cl(22)}{row_idx}", "values": [[match_data["clicks"]]]},
                ])
                actualizados += 1
            else:
                no_match.append(str(row[0])[:60])

        return {"actualizados": actualizados, "no_match": no_match}
    except Exception as e:
        return {"error": str(e), "actualizados": 0, "no_match": []}

"""
TÉRRET · Frankenstein — FastAPI Backend
"""

import os
import io
import json
import time
import unicodedata
from typing import Optional

import pandas as pd
from fastapi import FastAPI, Request, Form, File, UploadFile, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from core import (
    MASTER_PASSWORD, ESTADOS, ESTADO_CONFIG,
    obtener_credenciales, get_gspread_client,
    load_sheet, load_active_ads, load_briefs,
    load_voice_overs, load_caracteristicas,
    get_chars_producto, extraer_creador, extraer_genero,
    guardar_anuncio, eliminar_anuncio, actualizar_estado_anuncio,
    actualizar_metricas_anuncio, guardar_o_actualizar_brief,
    eliminar_asset, append_to_sheet_full,
    guardar_caracteristica, eliminar_caracteristica,
    upload_video_to_drive, upload_final_to_drive, upload_vo_to_drive,
    get_drive_service, get_or_create_folder, borrar_archivo_drive,
    concatenar_videos, convertir_a_mp4_normalizado,
    obtener_metricas_meta, sincronizar_meta_con_sheets,
    META_ACCESS_TOKEN, META_AD_ACCOUNT_ID,
    SHEET_ID, DRIVE_ROOT_FOLDER_ID, SHARED_DRIVE_ID,
)

app = FastAPI(title="Térret Frankenstein")

# ── Middleware ──────────────────────────────────────────
SECRET_KEY = os.environ.get("SECRET_KEY", "terret-secret-2024-frankenstein")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# ── Static & Templates ──────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ── Cache simple en memoria ─────────────────────────────
_cache = {}
_cache_ttl = {}
CACHE_TTL = 120  # segundos

def cache_get(key):
    if key in _cache and time.time() < _cache_ttl.get(key, 0):
        return _cache[key]
    return None

def cache_set(key, value, ttl=CACHE_TTL):
    _cache[key] = value
    _cache_ttl[key] = time.time() + ttl

def cache_clear():
    _cache.clear()
    _cache_ttl.clear()

# ── Context helper ──────────────────────────────────────
def base_context(request: Request, producto_sel: str = None):
    df = cache_get("sheet")
    if df is None:
        df = load_sheet()
        cache_set("sheet", df)

    productos = sorted(df["producto"].dropna().unique().tolist()) if not df.empty else ["Sin producto"]
    if not productos:
        productos = ["Sin producto"]

    sel = producto_sel or request.session.get("producto", productos[0])
    if sel not in productos and productos:
        sel = productos[0]

    return {
        "request":      request,
        "session":      request.session,
        "master":       request.session.get("master", False),
        "productos":    productos,
        "producto_sel": sel,
        "ESTADOS":      ESTADOS,
        "ESTADO_CONFIG": ESTADO_CONFIG,
    }

# ══════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════
@app.post("/login")
async def login(request: Request, password: str = Form(...)):
    if password == MASTER_PASSWORD:
        request.session["master"]        = True
        request.session["authenticated"] = True
    elif password:
        request.session["master"]        = False
        request.session["authenticated"] = True
    return RedirectResponse("/", status_code=302)

@app.post("/api/auth/login")
async def api_login(request: Request, data: dict):
    password = data.get("password", "")
    if password == MASTER_PASSWORD:
        request.session["master"]        = True
        request.session["authenticated"] = True
        return {"ok": True, "master": True}
    return {"ok": False}

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)

@app.post("/api/producto")
async def set_producto(request: Request, data: dict):
    request.session["producto"] = data.get("producto", "")
    return {"ok": True}

# ══════════════════════════════════════════════════════
# PÁGINAS PRINCIPALES
# ══════════════════════════════════════════════════════
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    ctx = base_context(request)
    if ctx["master"]:
        return RedirectResponse("/dashboard", status_code=302)
    return RedirectResponse("/bodega", status_code=302)

@app.get("/dashboard", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    ctx = base_context(request)
    if not ctx["master"]:
        return RedirectResponse("/bodega")

    df_ads = cache_get("ads")
    if df_ads is None:
        df_ads = load_active_ads()
        cache_set("ads", df_ads)
    
    # Saneamiento y blindaje
    df_ads = df_ads.fillna("").astype(str) if not df_ads.empty else df_ads

    ctx["df_ads"] = df_ads.to_dict("records") if not df_ads.empty else []
    return templates.TemplateResponse("dashboard.html", ctx)

@app.get("/bodega", response_class=HTMLResponse)
async def page_bodega(request: Request):
    ctx = base_context(request)
    
    df = cache_get("sheet")
    df = df if df is not None else load_sheet()
    cache_set("sheet", df)

    prod = ctx["producto_sel"]
    df_prod = df[df["producto"] == prod].copy() if not df.empty else pd.DataFrame()
    
    for col in ["genero", "descripcion", "caracteristica"]:
        if col not in df_prod.columns:
            df_prod[col] = ""
            
    # 🔥 Saneamiento de NaNs y forzado a String puro para Jinja2 🔥
    if not df_prod.empty:
        df_prod = df_prod.fillna("").astype(str)

    ctx["hooks"]  = df_prod[df_prod["tipo"] == "Hook"].to_dict("records") if not df_prod.empty else []
    ctx["bodies"] = df_prod[df_prod["tipo"] == "Body"].to_dict("records") if not df_prod.empty else []
    ctx["ctas"]   = df_prod[df_prod["tipo"] == "CTA"].to_dict("records")  if not df_prod.empty else []

    df_chars = cache_get("chars")
    df_chars = df_chars if df_chars is not None else load_caracteristicas()
    cache_set("chars", df_chars)
    ctx["caracteristicas"] = get_chars_producto(prod, df, df_chars)

    return templates.TemplateResponse("bodega.html", ctx)

@app.get("/mixer", response_class=HTMLResponse)
async def page_mixer(request: Request):
    ctx = base_context(request)
    if not ctx["master"]:
        return RedirectResponse("/bodega")

    df = cache_get("sheet")
    df = df if df is not None else load_sheet()
    cache_set("sheet", df)
    
    prod    = ctx["producto_sel"]
    df_prod = df[df["producto"] == prod].copy() if not df.empty else pd.DataFrame()

    for col in ["genero", "descripcion", "caracteristica"]:
        if col not in df_prod.columns:
            df_prod[col] = ""

    # 🔥 Saneamiento de NaNs y forzado a String puro para Jinja2 🔥
    if not df_prod.empty:
        df_prod = df_prod.fillna("").astype(str)

    hooks  = df_prod[df_prod["tipo"] == "Hook"].to_dict("records") if not df_prod.empty else []
    bodies = df_prod[df_prod["tipo"] == "Body"].to_dict("records") if not df_prod.empty else []
    ctas   = df_prod[df_prod["tipo"] == "CTA"].to_dict("records")  if not df_prod.empty else []

    # Características únicas
    caracts = []
    if not df_prod.empty and "caracteristica" in df_prod.columns:
        caracts = sorted([c for c in df_prod[df_prod["tipo"]=="Body"]["caracteristica"].unique()
                          if str(c).strip() and str(c).lower() != "nan"])

    ctx.update({
        "hooks": hooks, "bodies": bodies, "ctas": ctas,
        "caracteristicas": caracts,
        "n_hooks": len(hooks), "n_bodies": len(bodies), "n_ctas": len(ctas),
    })
    return templates.TemplateResponse("mixer.html", ctx)

@app.get("/produccion", response_class=HTMLResponse)
async def page_produccion(request: Request):
    ctx = base_context(request)
    
    df_ads = cache_get("ads")
    df_ads = df_ads if df_ads is not None else load_active_ads()
    cache_set("ads", df_ads)
    
    # Saneamiento y blindaje
    df_ads = df_ads.fillna("").astype(str) if not df_ads.empty else df_ads
    
    ctx["ads"] = df_ads.to_dict("records") if not df_ads.empty else []
    return templates.TemplateResponse("produccion.html", ctx)

@app.get("/briefs", response_class=HTMLResponse)
async def page_briefs(request: Request):
    ctx = base_context(request)
    df_briefs = load_briefs()
    prod = ctx["producto_sel"]
    brief = {}
    if not df_briefs.empty and "producto" in df_briefs.columns:
        row = df_briefs[df_briefs["producto"] == prod]
        if not row.empty:
            brief = row.fillna("").astype(str).iloc[0].to_dict()
    ctx["brief"] = brief
    return templates.TemplateResponse("briefs.html", ctx)

@app.get("/subir", response_class=HTMLResponse)
async def page_subir(request: Request):
    ctx = base_context(request)
    if not ctx["master"]:
        return RedirectResponse("/bodega")
        
    df = cache_get("sheet")
    df = df if df is not None else load_sheet()
    
    ctx["tipos_body"] = get_chars_producto(ctx["producto_sel"], df)
    return templates.TemplateResponse("subir.html", ctx)

@app.get("/voice-overs", response_class=HTMLResponse)
async def page_vos(request: Request):
    ctx = base_context(request)
    if not ctx["master"]:
        return RedirectResponse("/bodega")
    df_vos = load_voice_overs()
    prod = ctx["producto_sel"]
    
    df_vos = df_vos.fillna("").astype(str) if not df_vos.empty else df_vos
    
    ctx["vos"] = df_vos[df_vos["producto"]==prod].to_dict("records") if not df_vos.empty and "producto" in df_vos.columns else []
    return templates.TemplateResponse("voice_overs.html", ctx)

@app.get("/productos", response_class=HTMLResponse)
async def page_productos(request: Request):
    ctx = base_context(request)
    if not ctx["master"]:
        return RedirectResponse("/bodega")
        
    df = cache_get("sheet")
    df = df if df is not None else load_sheet()
    
    df_chars = cache_get("chars")
    df_chars = df_chars if df_chars is not None else load_caracteristicas()
    cache_set("chars", df_chars)
    
    df_chars = df_chars.fillna("").astype(str) if not df_chars.empty else df_chars
    
    ctx["df_chars"] = df_chars.to_dict("records") if not df_chars.empty else []
    return templates.TemplateResponse("productos.html", ctx)

# ══════════════════════════════════════════════════════
# API — DATOS
# ══════════════════════════════════════════════════════
@app.get("/api/sheet")
async def api_sheet(request: Request):
    df = load_sheet()
    cache_set("sheet", df)
    return df.fillna("").astype(str).to_dict("records")

@app.get("/api/ads")
async def api_ads():
    df = load_active_ads()
    cache_set("ads", df)
    return df.fillna("").astype(str).to_dict("records")

@app.get("/api/dashboard-data")
async def api_dashboard_data(request: Request):
    """Datos agregados para el dashboard."""
    df_ads = load_active_ads()
    if df_ads.empty:
        return {"ads": [], "stats": {}, "pipeline": {}}

    df_pub = df_ads[df_ads["estado"] == "Publicado"]
    total_inv  = float(df_pub["inversion"].sum())
    total_ing  = float(df_pub["ingresos"].sum())
    total_comp = int(df_pub["compras"].sum())
    roas_global = round(total_ing / total_inv, 2) if total_inv > 0 else 0

    pipeline = {e: int((df_ads["estado"] == e).sum()) for e in ESTADOS}

    df_ads = df_ads.fillna("").astype(str)
    df_pub = df_pub.fillna("").astype(str)

    return {
        "ads": df_ads.to_dict("records"),
        "ads_pub": df_pub.to_dict("records"),
        "stats": {
            "inversion": total_inv,
            "ingresos":  total_ing,
            "compras":   total_comp,
            "roas":      roas_global,
        },
        "pipeline": pipeline,
    }

@app.get("/api/bodega-data")
async def api_bodega_data(producto: str):
    df = load_sheet()
    if df.empty:
        return {"hooks": [], "bodies": [], "ctas": []}
    df_prod = df[df["producto"] == producto].copy()
    for col in ["genero", "descripcion", "caracteristica"]:
        if col not in df_prod.columns:
            df_prod[col] = ""
            
    df_prod = df_prod.fillna("").astype(str)
    
    return {
        "hooks":  df_prod[df_prod["tipo"] == "Hook"].to_dict("records"),
        "bodies": df_prod[df_prod["tipo"] == "Body"].to_dict("records"),
        "ctas":   df_prod[df_prod["tipo"] == "CTA"].to_dict("records"),
    }

# ══════════════════════════════════════════════════════
# API — ACCIONES
# ══════════════════════════════════════════════════════
@app.post("/api/anuncio/guardar")
async def api_guardar_anuncio(request: Request):
    data = await request.json()
    ok = guardar_anuncio([
        data.get("nombre_anuncio", ""),
        data.get("hook", ""),
        data.get("body", ""),
        data.get("cta", ""),
        data.get("genero_hook", ""),
        data.get("genero_body", ""),
        data.get("genero_cta", ""),
        0.0, 0, 0.0, 0.0,
        data.get("estado", "Pendiente"),
        "",
    ])
    cache_clear()
    return {"ok": ok}

@app.post("/api/anuncio/estado")
async def api_estado_anuncio(request: Request):
    data = await request.json()
    ok = actualizar_estado_anuncio(
        data["nombre"],
        data["estado"],
        data.get("video_url", ""),
        data.get("razon", ""),
    )
    cache_clear()
    return {"ok": ok}

@app.post("/api/anuncio/eliminar")
async def api_eliminar_anuncio(request: Request):
    data  = await request.json()
    ok    = eliminar_anuncio(data["nombre"])
    cache_clear()
    return {"ok": ok}

@app.post("/api/anuncio/metricas")
async def api_metricas(request: Request):
    data = await request.json()
    ok   = actualizar_metricas_anuncio(
        data["nombre"],
        data.get("inversion", 0),
        data.get("compras", 0),
        data.get("ctr", 0),
        data.get("roas", 0),
        data.get("hook_rate", 0),
        data.get("hold_rate", 0),
        data.get("cpa", 0),
    )
    cache_clear()
    return {"ok": ok}

@app.post("/api/meta/sync")
async def api_meta_sync(request: Request):
    if not request.session.get("master"):
        raise HTTPException(403)
    respuesta = obtener_metricas_meta(META_ACCESS_TOKEN, META_AD_ACCOUNT_ID)
    if not respuesta["exito"]:
        return {"ok": False, "error": respuesta["error"]}
    resultado = sincronizar_meta_con_sheets(respuesta["datos"])
    cache_clear()
    return {"ok": True, **resultado}

@app.post("/api/meta/import-csv")
async def api_meta_import_csv(request: Request, file: UploadFile = File(...)):
    if not request.session.get("master"):
        raise HTTPException(403)
    try:
        content = await file.read()
        df_meta = pd.read_csv(io.StringIO(content.decode("utf-8")))
        df_meta.columns = [c.strip() for c in df_meta.columns]

        cols_req = ["Nombre del anuncio", "CTR (todos)", "Hold Rate",
                    "Hook rate", "Results ROAS", "Resultados", "Importe gastado (COP)"]
        faltantes = [c for c in cols_req if c not in df_meta.columns]
        if faltantes:
            return {"ok": False, "error": f"Columnas faltantes: {faltantes}"}

        df_meta["Importe gastado (COP)"] = pd.to_numeric(
            df_meta["Importe gastado (COP)"], errors="coerce").fillna(0)
        df_meta["inversion"]  = df_meta["Importe gastado (COP)"]  # COP directo
        df_meta["hook_rate"]  = pd.to_numeric(df_meta["Hook rate"],      errors="coerce").fillna(0) * 100
        df_meta["hold_rate"]  = pd.to_numeric(df_meta["Hold Rate"],      errors="coerce").fillna(0) * 100
        df_meta["ctr"]        = pd.to_numeric(df_meta["CTR (todos)"],    errors="coerce").fillna(0)
        df_meta["roas"]       = pd.to_numeric(df_meta["Results ROAS"],   errors="coerce").fillna(0)
        df_meta["compras"]    = pd.to_numeric(df_meta["Resultados"],     errors="coerce").fillna(0)

        df_valido = df_meta[df_meta["inversion"] > 0].copy().fillna("").astype(str)

        # Preview para el frontend
        preview = df_valido[["Nombre del anuncio","inversion","compras","ctr","hook_rate","hold_rate","roas"]].to_dict("records")

        return {"ok": True, "preview": preview, "total": len(df_valido)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/meta/import-csv/apply")
async def api_meta_import_apply(request: Request):
    if not request.session.get("master"):
        raise HTTPException(403)
    data = await request.json()
    rows = data.get("rows", [])

    def limpiar(t):
        s = str(t).strip().lower().replace(" ","").replace("_","").replace("+","")
        return "".join(c for c in unicodedata.normalize("NFD", s)
                       if unicodedata.category(c) != "Mn")
    try:
        client    = get_gspread_client()
        sheet     = client.open_by_key(SHEET_ID).worksheet("Anuncios_Activos")
        all_rows  = sheet.get_all_values()
        actualizados = 0
        no_match     = []

        for row_data in rows:
            nombre    = str(row_data["Nombre del anuncio"])
            meta_key  = limpiar(nombre)
            matched   = None
            for ridx, srow in enumerate(all_rows[1:], start=2):
                if not srow: continue
                sk = limpiar(str(srow[0]))
                if sk in meta_key or meta_key in sk:
                    matched = ridx
                    break

            if matched:
                inv  = float(row_data.get("inversion",  0))
                comp = int(row_data.get("compras",   0))
                ctr  = round(float(row_data.get("ctr",      0)), 2)
                roas = round(float(row_data.get("roas",     0)), 2)
                hr   = round(float(row_data.get("hook_rate",0)), 2)
                hold = round(float(row_data.get("hold_rate",0)), 2)
                cpa  = round(inv / comp, 2) if comp > 0 else 0

                def cl(n): return chr(64+n)
                sheet.batch_update([
                    {"range": f"{cl(8)}{matched}",  "values": [[inv]]},
                    {"range": f"{cl(9)}{matched}",  "values": [[comp]]},
                    {"range": f"{cl(10)}{matched}", "values": [[ctr]]},
                    {"range": f"{cl(11)}{matched}", "values": [[roas]]},
                    {"range": f"{cl(15)}{matched}", "values": [[hr]]},
                    {"range": f"{cl(16)}{matched}", "values": [[hold]]},
                    {"range": f"{cl(19)}{matched}", "values": [[cpa]]},
                ])
                actualizados += 1
            else:
                no_match.append(nombre[:60])

        cache_clear()
        return {"ok": True, "actualizados": actualizados, "no_match": no_match}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/brief/guardar")
async def api_guardar_brief(request: Request):
    data = await request.json()
    ok = guardar_o_actualizar_brief(
        data["producto"], data.get("hooks",""),
        data.get("bodies",""), data.get("ctas",""),
        data.get("guiones","")
    )
    return {"ok": ok}

@app.post("/api/asset/eliminar")
async def api_eliminar_asset(request: Request):
    data = await request.json()
    ok   = eliminar_asset(data["nombre"])
    cache_clear()
    return {"ok": ok}

@app.post("/api/asset/subir")
async def api_subir_asset(
    request: Request,
    file: UploadFile = File(...),
    producto: str = Form(...),
    tipo: str = Form(...),
    creador: str = Form(...),
    genero: str = Form(...),
    caracteristica: str = Form(""),
    descripcion: str = Form(""),
):
    if not request.session.get("master"):
        raise HTTPException(403)
    try:
        file_bytes = await file.read()
        ext        = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "mp4"

        # Consecutivo
        df = load_sheet()
        consecutivo = len(df[df["tipo"] == tipo]) + 1 if not df.empty else 1
        creador_limpio = creador.strip().title().replace(" ", "")
        nombre_base = f"{tipo}_{creador_limpio}_{genero}_{consecutivo}"
        filename    = f"{nombre_base}.{ext}"

        video_link = upload_video_to_drive(file_bytes, filename, producto, tipo)

        row = [producto, tipo, nombre_base, video_link,
               descripcion, genero, caracteristica, "", ""]
        ok  = append_to_sheet_full(row)
        cache_clear()
        return {"ok": ok, "nombre": nombre_base, "link": video_link}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/mixer/ensamblar")
async def api_ensamblar(request: Request):
    if not request.session.get("master"):
        raise HTTPException(403)
    data = await request.json()

    hook_nombre = data.get("hook")
    body_nombres = data.get("bodies", [])
    cta_nombre  = data.get("cta")
    producto    = data.get("producto")

    df = load_sheet()
    url_col = "video_url" if "video_url" in df.columns else "url"

    def get_url(nombre):
        row = df[df["nombre"] == nombre]
        return str(row.iloc[0].get(url_col, "")) if not row.empty else ""

    clips = []
    if hook_nombre:
        clips.append((hook_nombre, get_url(hook_nombre)))
    for b in body_nombres:
        clips.append((b, get_url(b)))
    if cta_nombre:
        clips.append((cta_nombre, get_url(cta_nombre)))

    # Descargar clips de Drive
    from core import download_file_from_drive, get_drive_service, extract_file_id
    service = get_drive_service()
    rutas   = []
    try:
        for nombre, url in clips:
            fid = extract_file_id(url)
            if not fid:
                return {"ok": False, "error": f"Sin URL válida para {nombre}"}
            path = f"tmp_{nombre[:20]}.mp4"
            download_file_from_drive(service, fid, path)
            rutas.append(path)

        output = f"final_{int(time.time())}.mp4"
        exito, err = concatenar_videos(rutas, output)
        if not exito:
            return {"ok": False, "error": err}

        with open(output, "rb") as f:
            final_bytes = f.read()

        # Construir nombre del anuncio
        hook_label   = hook_nombre or "SinHook"
        cta_label    = cta_nombre  or "SinCTA"
        nombre_auto  = f"{producto}_{hook_label}_{'+ '.join(body_nombres)}_{cta_label}"

        final_link = upload_final_to_drive(final_bytes, f"{nombre_auto}.mp4", producto)

        # Registrar
        g_hook = extraer_genero(hook_nombre) if hook_nombre else "N/A"
        g_cta  = extraer_genero(cta_nombre)  if cta_nombre  else "N/A"
        g_bod  = [extraer_genero(b) for b in body_nombres]
        g_body_final = "Mix" if len(set(g_bod)) > 1 else (g_bod[0] if g_bod else "N/A")
        body_str = " + ".join(body_nombres)

        guardar_anuncio([nombre_auto, hook_label, body_str, cta_label,
                         g_hook, g_body_final, g_cta,
                         0.0, 0, 0.0, 0.0, "Listo", final_link])
        actualizar_estado_anuncio(nombre_auto, "Listo", final_link)
        cache_clear()

        return {"ok": True, "nombre": nombre_auto, "link": final_link}
    finally:
        import os as _os
        for r in rutas:
            if _os.path.exists(r): _os.remove(r)
        if _os.path.exists(output): _os.remove(output)

@app.post("/api/vo/subir")
async def api_subir_vo(
    request: Request,
    file: UploadFile = File(...),
    producto: str = Form(...),
    locutor: str = Form(...),
    descripcion: str = Form(""),
    idioma: str = Form("Español"),
):
    if not request.session.get("master"):
        raise HTTPException(403)
    try:
        file_bytes = await file.read()
        ext  = file.filename.rsplit(".", 1)[-1].lower() if "." in file.filename else "mp3"
        nombre = f"VO_{locutor.strip().replace(' ','')}_{producto}_{int(time.time())}.{ext}"
        link = upload_vo_to_drive(file_bytes, nombre, producto, ext)

        client = get_gspread_client()
        ws = client.open_by_key(SHEET_ID).worksheet("VoiceOvers")
        ws.append_row([producto, nombre, link, descripcion, locutor, "", idioma,
                       time.strftime("%Y-%m-%d")])
        return {"ok": True, "link": link}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/caracteristica/agregar")
async def api_add_char(request: Request):
    data = await request.json()
    ok   = guardar_caracteristica(data["producto"], data["caracteristica"])
    cache_clear()
    return {"ok": ok}

@app.post("/api/caracteristica/eliminar")
async def api_del_char(request: Request):
    data = await request.json()
    ok   = eliminar_caracteristica(data["producto"], data["caracteristica"])
    cache_clear()
    return {"ok": ok}

@app.post("/api/cache/clear")
async def api_cache_clear():
    cache_clear()
    return {"ok": True}

# ── Health check ────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "app": "Térret Frankenstein"}

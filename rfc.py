# -*- coding: utf-8 -*-
import os
import sys
import re
import ssl
import tempfile
from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
from zipfile import ZipFile

import qrcode
import requests
from bs4 import BeautifulSoup
from docx import Document
from flask import Flask, request, send_file, jsonify
from flask_cors import CORS
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

import secrets
from werkzeug.security import generate_password_hash, check_password_hash

from docx_to_pdf_aspose import docx_to_pdf_aspose

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
    
from stats_store import get_and_update, get_state

WA_PROCESSED_MSG_IDS = set()

# ====== STATS PERSISTENTES ======
STATS_PATH = os.getenv("STATS_PATH", "/data/stats.json")  # Render Disk: /data
ADMIN_STATS_TOKEN = os.getenv("ADMIN_STATS_TOKEN", "")    # opcional para proteger /admin y /stats

# ================== ADAPTADOR TLS SAT ==================

class SATAdapter(HTTPAdapter):
    """
    Adaptador para forzar un contexto TLS que no use DH de clave pequeña.
    """
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('HIGH:!DH:!aNULL')
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

# ================== CONSTANTES ==================

MESES_ES = {
    1: "ENERO",
    2: "FEBRERO",
    3: "MARZO",
    4: "ABRIL",
    5: "MAYO",
    6: "JUNIO",
    7: "JULIO",
    8: "AGOSTO",
    9: "SEPTIEMBRE",
    10: "OCTUBRE",
    11: "NOVIEMBRE",
    12: "DICIEMBRE",
}

# ================== USUARIOS / SESIONES / IP / LÍMITES ==================
# CAMBIA ESTO por tus usuarios reales
USERS = {
    # usuario : contraseña (en claro, pero se guarda como hash)
    "admin": generate_password_hash("Loc0722E02"),
    "graciela.barajas": generate_password_hash("BarajasCIF26"),
    "eos": generate_password_hash("EOScif26"),
    "gerardo.calzada": generate_password_hash("CalzadaIDCIF26"),
    # "papeleria_lupita": generate_password_hash("clave_lupita"),
    # "abogados_lopez": generate_password_hash("clave_lopez"),
}

# username -> token activo (solo 1 sesión por usuario)
ACTIVE_SESSIONS = {}

# token -> username
TOKEN_TO_USER = {}

# Historial de logins por usuario
# {"usuario": [{"ip": "...", "fecha": "...", "ua": "..."}]}
HISTORIAL_LOGIN = {}

# Info de IP por usuario, para poder fijar una IP si quieres
# {"usuario": {"ip": "...", "bloquear_otras": True/False}}
USERS_IP_INFO = {}

# Si True, la primera IP que use el usuario se fija y se bloquean otras
BLOQUEAR_IP_POR_DEFAULT = False  # déjalo False mientras solo observas

# Límite diario de constancias por usuario
USO_POR_USUARIO = {}  # {"usuario": {"hoy": "2025-12-31", "count": 3}}
LIMITE_DIARIO = 50    # cambia este número según tu plan

# ================== FUNCIONES AUXILIARES ==================

def hoy_mexico():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).date()
    except Exception:
        return datetime.utcnow().date()

def formatear_fecha_dd_de_mmmm_de_aaaa(d_str, sep="-"):
    if not d_str:
        return ""
    partes = d_str.strip().split(sep)
    if len(partes) != 3:
        return d_str
    dd, mm, yyyy = partes
    try:
        dia = int(dd)
        mes = int(mm)
        anio = int(yyyy)
    except ValueError:
        return d_str
    nombre_mes = MESES_ES.get(mes, mm)
    return f"{dia:02d} DE {nombre_mes} DE {anio}"

def fecha_actual_lugar(localidad, entidad):
    hoy = hoy_mexico()
    dia = str(hoy.day).zfill(2)
    mes = MESES_ES[hoy.month]
    anio = hoy.year

    loc = (localidad or "").upper()
    ent = (entidad or "").upper()

    if not loc and not ent:
        lugar = ""
    elif loc and ent:
        lugar = f"{loc} , {ent} A "
    elif loc:
        lugar = f"{loc} A "
    else:
        lugar = f"{ent} A "

    return f"{lugar}{dia} DE {mes} DE {anio}"

def get_client_ip():
    """
    Intenta obtener la IP real del cliente, tomando en cuenta proxies (Render).
    """
    if "X-Forwarded-For" in request.headers:
        return request.headers["X-Forwarded-For"].split(",")[0].strip()
    return request.remote_addr or "0.0.0.0"

def generar_qr_y_barcode(url_qr, rfc):
    # --- QR ---
    qr = qrcode.QRCode(
        version=None,
        box_size=8,
        border=2,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
    )
    qr.add_data(url_qr)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white")

    buf_qr = BytesIO()
    qr_img.save(buf_qr, format="PNG")
    qr_bytes = buf_qr.getvalue()

    # --- Código de barras (servicio externo) ---
    import urllib.parse
    rfc_encoded = urllib.parse.quote_plus(rfc)

    url_barcode = (
        "https://barcode.tec-it.com/barcode.ashx"
        f"?data={rfc_encoded}"
        "&code=Code128"
        "&translate-esc=on"
        "&dpi=300"
    )
    resp_barcode = requests.get(url_barcode, timeout=20)
    resp_barcode.raise_for_status()
    barcode_bytes = resp_barcode.content

    return qr_bytes, barcode_bytes

def obtener_mapa_trs(soup):
    filas = {}
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            etiqueta = tds[0].get_text(strip=True)
            valor = tds[1].get_text(strip=True)
            if etiqueta:
                filas[etiqueta] = valor
    return filas

def extraer_datos_desde_sat(rfc, idcif):
    d3 = f"{idcif}_{rfc}"

    url = "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
    params = {"D1": "10", "D2": "1", "D3": d3}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    session = requests.Session()
    session.mount("https://siat.sat.gob.mx", SATAdapter())

    resp = session.get(url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    mapa = obtener_mapa_trs(soup)

    def get_val(*keys_posibles):
        for k in keys_posibles:
            if k in mapa:
                return mapa[k]
        return ""

    nombre = get_val("Nombre:", "Nombre (s):")
    ape1 = get_val("Apellido Paterno:", "Primer Apellido:")
    ape2 = get_val("Apellido Materno:", "Segundo Apellido:")
    nombre_etiqueta = " ".join(x for x in [nombre, ape1, ape2] if x).strip()

    fecha_inicio_raw = get_val("Fecha de Inicio de operaciones:", "Fecha inicio de operaciones:")
    fecha_ultimo_raw = get_val("Fecha del último cambio de situación:", "Fecha de último cambio de estado:")

    fecha_inicio_texto = formatear_fecha_dd_de_mmmm_de_aaaa(fecha_inicio_raw, sep="-")
    fecha_ultimo_texto = formatear_fecha_dd_de_mmmm_de_aaaa(fecha_ultimo_raw, sep="-")

    estatus = get_val("Situación del contribuyente:", "Estatus en el padrón:")
    curp = get_val("CURP:")

    cp = get_val("CP:", "Código Postal:")
    tipo_vialidad = get_val("Tipo de vialidad:")
    vialidad = get_val("Nombre de la vialidad:")
    no_ext = get_val("Número exterior:")
    no_int = get_val("Número interior:")
    colonia = get_val("Colonia:", "Nombre de la Colonia:")
    localidad = get_val("Municipio o delegación:", "Nombre del Municipio o Demarcación Territorial:")
    entidad = get_val("Entidad Federativa:", "Nombre de la Entidad Federativa:")

    regimen = get_val("Régimen:")
    fecha_alta_raw = get_val("Fecha de alta:")
    fecha_alta = fecha_alta_raw.replace("-", "/") if fecha_alta_raw else ""

    if not any([nombre, ape1, ape2, curp, cp, regimen]):
        raise ValueError("SIN_DATOS_SAT")

    fecha_actual = fecha_actual_lugar(localidad, entidad)

    ahora = datetime.now(ZoneInfo("America/Mexico_City"))
    fecha_corta = ahora.strftime("%Y/%m/%d %H:%M:%S")

    datos = {
        "RFC_ETIQUETA": rfc,
        "NOMBRE_ETIQUETA": nombre_etiqueta,
        "IDCIF_ETIQUETA": idcif,

        "RFC": rfc,
        "CURP": curp,
        "NOMBRE": nombre,
        "PRIMER_APELLIDO": ape1,
        "SEGUNDO_APELLIDO": ape2,
        "FECHA_INICIO": fecha_inicio_texto,
        "ESTATUS": estatus,
        "FECHA_ULTIMO": fecha_ultimo_texto,
        "FECHA": fecha_actual,
        "FECHA_CORTA": fecha_corta,

        "CP": cp,
        "TIPO_VIALIDAD": tipo_vialidad,
        "VIALIDAD": vialidad,
        "NO_EXTERIOR": no_ext,
        "NO_INTERIOR": no_int,
        "COLONIA": colonia,
        "LOCALIDAD": localidad,
        "ENTIDAD": entidad,

        "REGIMEN": regimen,
        "FECHA_ALTA": fecha_alta,
    }

    return datos

def reemplazar_en_documento(ruta_entrada, ruta_salida, datos):
    rfc_val = datos.get("RFC_ETIQUETA") or datos.get("RFC", "")
    idcif_val = datos.get("IDCIF_ETIQUETA", "")

    d3 = f"{idcif_val}_{rfc_val}"
    url_qr = (
        "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
        f"?D1=10&D2=1&D3={d3}"
    )

    qr_bytes, barcode_bytes = generar_qr_y_barcode(url_qr, rfc_val)

    placeholders = {
        "{{ RFC ETIQUETA }}": datos.get("RFC_ETIQUETA", ""),
        "{{ NOMBRE ETIQUETA }}": datos.get("NOMBRE_ETIQUETA", ""),
        "{{ idCIF }}": datos.get("IDCIF_ETIQUETA", ""),
        "{{ FECHA }}": datos.get("FECHA", ""),
        "{{ FECHA CORTA }}": datos.get("FECHA_CORTA", ""),
        "{{ RFC }}": datos.get("RFC", ""),
        "{{ CURP }}": datos.get("CURP", ""),
        "{{ NOMBRE }}": datos.get("NOMBRE", ""),
        "{{ PRIMER APELLIDO }}": datos.get("PRIMER_APELLIDO", ""),
        "{{ SEGUNDO APELLIDO }}": datos.get("SEGUNDO_APELLIDO", ""),
        "{{ FECHA INICIO }}": datos.get("FECHA_INICIO", ""),
        "{{ ESTATUS }}": datos.get("ESTATUS", ""),
        "{{ FECHA ULTIMO }}": datos.get("FECHA_ULTIMO", ""),
        "{{ CP }}": datos.get("CP", ""),
        "{{ TIPO VIALIDAD }}": datos.get("TIPO_VIALIDAD", ""),
        "{{ VIALIDAD }}": datos.get("VIALIDAD", ""),
        "{{ NO EXTERIOR }}": datos.get("NO_EXTERIOR", ""),
        "{{ NO INTERIOR }}": datos.get("NO_INTERIOR", ""),
        "{{ COLONIA }}": datos.get("COLONIA", ""),
        "{{ LOCALIDAD }}": datos.get("LOCALIDAD", ""),
        "{{ ENTIDAD }}": datos.get("ENTIDAD", ""),
        "{{ REGIMEN }}": datos.get("REGIMEN", ""),
        "{{ FECHA ALTA }}": datos.get("FECHA_ALTA", ""),
    }

    with ZipFile(ruta_entrada, "r") as zin, ZipFile(ruta_salida, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)

            if (
                item.filename == "word/document.xml"
                or item.filename.startswith("word/header")
                or item.filename.startswith("word/footer")
            ):
                try:
                    xml_text = data.decode("utf-8")
                except UnicodeDecodeError:
                    pass
                else:
                    if idcif_val:
                        patron_idcif = r"<w:t>{{</w:t>.*?<w:t>idCIF</w:t>.*?<w:t>}}</w:t>"
                        xml_text, _ = re.subn(
                            patron_idcif,
                            f"<w:t>{idcif_val}</w:t>",
                            xml_text,
                            flags=re.DOTALL
                        )

                    for k, v in placeholders.items():
                        if k in xml_text:
                            xml_text = xml_text.replace(k, v)

                    data = xml_text.encode("utf-8")

            if item.filename == "word/media/image2.png":
                data = qr_bytes
            elif item.filename == "word/media/image6.png":
                data = barcode_bytes

            zout.writestr(item, data)

    doc = Document(ruta_salida)

    par_placeholders = {
        "{{ FECHA CORTA }}": datos.get("FECHA_CORTA", ""),
        "{{FECHA CORTA}}": datos.get("FECHA_CORTA", ""),
        "{{ FECHA }}": datos.get("FECHA", ""),
        "{{FECHA}}": datos.get("FECHA", ""),
        "{{ RFC }}": datos.get("RFC", ""),
        "{{RFC}}": datos.get("RFC", ""),
        "{{ idCIF }}": datos.get("IDCIF_ETIQUETA", ""),
        "{{idCIF}}": datos.get("IDCIF_ETIQUETA", ""),
    }

    def reemplazar_en_parrafos(paragraphs):
        for p in paragraphs:
            if "{{" not in p.text:
                continue

            full = "".join(r.text for r in p.runs)
            if "{{" not in full:
                continue

            start_idx = full.find("{{")
            if start_idx == -1:
                continue

            acc = 0
            start_run = None
            for i, r in enumerate(p.runs):
                if acc + len(r.text) > start_idx:
                    start_run = i
                    break
                acc += len(r.text)

            if start_run is None:
                continue

            suffix = "".join(r.text for r in p.runs[start_run:])
            new_suffix = suffix
            for k, v in par_placeholders.items():
                if k in new_suffix:
                    new_suffix = new_suffix.replace(k, v)

            if new_suffix == suffix:
                continue

            p.runs[start_run].text = new_suffix
            for r in p.runs[start_run + 1:]:
                r.text = ""

    reemplazar_en_parrafos(doc.paragraphs)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                reemplazar_en_parrafos(cell.paragraphs)

    doc.save(ruta_salida)

# ================== AUTH HELPERS ==================

def crear_sesion(username: str) -> str:
    """
    Crea un token nuevo para el usuario.
    (Aquí ya asumimos que solo tendrá 1 sesión y el login revisa eso)
    """
    token = secrets.token_urlsafe(32)

    token_anterior = ACTIVE_SESSIONS.get(username)
    if token_anterior:
        TOKEN_TO_USER.pop(token_anterior, None)

    ACTIVE_SESSIONS[username] = token
    TOKEN_TO_USER[token] = username
    return token

def obtener_usuario_desde_token(token: str):
    username = TOKEN_TO_USER.get(token)
    if not username:
        return None
    if ACTIVE_SESSIONS.get(username) != token:
        return None
    return username

def usuario_actual_o_none():
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(" ", 1)[1].strip()
    if not token:
        return None
    return obtener_usuario_desde_token(token)

# ================== APP FLASK ==================

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://constancia-7xk29.vercel.app"}})

REQUEST_TOTAL = 0
REQUEST_POR_DIA = {}
SUCCESS_COUNT = 0
SUCCESS_RFCS = []
WA_VERIFY_TOKEN = os.getenv("WA_VERIFY_TOKEN", "mi_token_wa_2026")

@app.route("/wa/webhook", methods=["GET"])
def wa_webhook_verify():
    mode = request.args.get("hub.mode", "")
    token = request.args.get("hub.verify_token", "")
    challenge = request.args.get("hub.challenge", "")

    print("WA VERIFY GET -> mode:", mode, "token:", token, "challenge:", challenge)

    if mode == "subscribe" and token == WA_VERIFY_TOKEN:
        return challenge, 200

    return "Forbidden", 403

# --- WhatsApp helpers mínimos ---
WA_TOKEN = os.getenv("WA_TOKEN", "")
WA_PHONE_NUMBER_ID = os.getenv("WA_PHONE_NUMBER_ID", "")
WA_GRAPH_VERSION = os.getenv("WA_GRAPH_VERSION", "v20.0")

print("WA CONFIG -> PHONE_NUMBER_ID:", WA_PHONE_NUMBER_ID)
print("WA CONFIG -> GRAPH_VERSION:", WA_GRAPH_VERSION)
print("WA CONFIG -> TOKEN_LEN:", len(WA_TOKEN))
print("WA CONFIG -> TOKEN_LAST6:", WA_TOKEN[-6:] if WA_TOKEN else "EMPTY")

def normalizar_wa_to(wa_id: str) -> str:
    """
    Meta a veces manda México como 521XXXXXXXXXX (formato viejo).
    En la consola de prueba, los destinatarios autorizados suelen estar como 52XXXXXXXXXX (sin el 1).
    Convertimos 521 + 10 dígitos -> 52 + 10 dígitos.
    """
    wa_id = (wa_id or "").strip()
    if wa_id.startswith("521") and len(wa_id) == 13:
        return "52" + wa_id[3:]
    return wa_id

def wa_api_url(path: str) -> str:
    return f"https://graph.facebook.com/{WA_GRAPH_VERSION}/{path.lstrip('/')}"

def wa_send_text(to_wa_id: str, text: str):
    if not (WA_TOKEN and WA_PHONE_NUMBER_ID):
        raise RuntimeError("Faltan WA_TOKEN o WA_PHONE_NUMBER_ID.")

    url = wa_api_url(f"{WA_PHONE_NUMBER_ID}/messages")
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_wa_id,
        "type": "text",
        "text": {"body": text},
    }

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    # 👇 esto te imprime el error exacto (muy importante)
    if not r.ok:
        print("WA SEND ERROR status:", r.status_code)
        print("WA SEND ERROR body:", r.text)

    r.raise_for_status()
    return r.json()

def extraer_rfc_idcif(texto: str):
    """
    Acepta formatos:
    - RFC: TOHJ640426XXX IDCIF: 19010347XXX
    - TOHJ640426XXX 19010347XXX
    - rfc TOHJ640426XXX idcif 19010347XXX
    """
    if not texto:
        return None, None

    t = texto.strip().upper()

    # RFC: 12 o 13 chars (persona física 13, moral 12)
    rfc_regex = r"\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b"
    # idCIF: normalmente numérico largo (pero lo dejamos flexible)
    idcif_regex = r"\b(\d{8,20})\b"

    # Caso con etiquetas
    rfc_m = re.search(r"(RFC\s*[:\-]?\s*)" + rfc_regex, t)
    id_m = re.search(r"(IDCIF\s*[:\-]?\s*)" + idcif_regex, t)

    rfc = rfc_m.group(2) if rfc_m else None
    idcif = id_m.group(2) if id_m else None

    # Caso sin etiquetas: buscar 1 RFC y 1 número
    if not rfc:
        m = re.search(rfc_regex, t)
        if m:
            rfc = m.group(1)

    if not idcif:
        nums = re.findall(idcif_regex, t)
        if nums:
            # toma el primero “largo”
            idcif = nums[0]

    return rfc, idcif


def wa_upload_document(file_bytes: bytes, filename: str, mime: str):
    """
    Sube un archivo a WhatsApp y regresa media_id
    """
    if not (WA_TOKEN and WA_PHONE_NUMBER_ID):
        raise RuntimeError("Faltan WA_TOKEN o WA_PHONE_NUMBER_ID.")

    url = wa_api_url(f"{WA_PHONE_NUMBER_ID}/media")
    headers = {"Authorization": f"Bearer {WA_TOKEN}"}
    files = {
        "file": (filename, file_bytes, mime),
    }
    data = {"messaging_product": "whatsapp"}
    r = requests.post(url, headers=headers, files=files, data=data, timeout=60)

    if not r.ok:
        print("WA MEDIA UPLOAD ERROR status:", r.status_code)
        print("WA MEDIA UPLOAD ERROR body:", r.text)

    r.raise_for_status()
    return r.json().get("id")


def wa_send_document(to_wa_id: str, media_id: str, filename: str, caption: str = ""):
    """
    Envía un documento ya subido (media_id) por WhatsApp
    """
    if not (WA_TOKEN and WA_PHONE_NUMBER_ID):
        raise RuntimeError("Faltan WA_TOKEN o WA_PHONE_NUMBER_ID.")

    url = wa_api_url(f"{WA_PHONE_NUMBER_ID}/messages")
    headers = {
        "Authorization": f"Bearer {WA_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to_wa_id,
        "type": "document",
        "document": {
            "id": media_id,
            "filename": filename,
        },
    }
    if caption:
        payload["document"]["caption"] = caption

    r = requests.post(url, headers=headers, json=payload, timeout=30)

    if not r.ok:
        print("WA SEND DOC ERROR status:", r.status_code)
        print("WA SEND DOC ERROR body:", r.text)

    r.raise_for_status()
    return r.json()

@app.route("/wa/webhook", methods=["POST"])
def wa_webhook_receive():
    payload = request.get_json(silent=True) or {}
    print("WA WEBHOOK POST payload:", payload)

    try:
        entry = (payload.get("entry") or [])[0]
        changes = (entry.get("changes") or [])[0]
        value = changes.get("value") or {}

        messages = value.get("messages") or []
        if not messages:
            return "OK", 200

        msg = messages[0]
        
        msg_id = msg.get("id")
        if msg_id and msg_id in WA_PROCESSED_MSG_IDS:
            print("WA DUPLICATE msg_id ignored:", msg_id)
            return "OK", 200
        if msg_id:
            WA_PROCESSED_MSG_IDS.add(msg_id)

        contacts = value.get("contacts") or []

        raw_wa_id = (contacts[0].get("wa_id") if contacts else None) or msg.get("from")
        from_wa_id = normalizar_wa_to(raw_wa_id)
        print("WA TO normalized:", raw_wa_id, "->", from_wa_id)

        msg_type = msg.get("type")

        text_body = ""
        if msg_type == "text":
            text_body = ((msg.get("text") or {}).get("body") or "").strip()

        if not from_wa_id:
            return "OK", 200

        if not text_body:
            wa_send_text(
                from_wa_id,
                "Envíame RFC e idCIF.\nEjemplo:\nRFC: TOHJ640426XXX\nIDCIF: 19010347XXX"
            )
            return "OK", 200

        # 1) Extraer RFC + idCIF
        rfc, idcif = extraer_rfc_idcif(text_body)

        if not rfc or not idcif:
            wa_send_text(
                from_wa_id,
                "✅ Recibí tu mensaje.\n\nAhora envíame RFC e idCIF en este formato:\n"
                "RFC: TOHJ640426XXX\nIDCIF: 19010347XXX\n\n"
                "Tip: si quieres también Word, escribe al final: DOCX o AMBOS"
            )
            return "OK", 200

        # 2) Avisar
        wa_send_text(from_wa_id, f"⏳ Generando constancia...\nRFC: {rfc}\nidCIF: {idcif}")

        # 3) SAT
        try:
            datos = extraer_datos_desde_sat(rfc, idcif)
        except ValueError as e:
            if str(e) == "SIN_DATOS_SAT":
                wa_send_text(
                    from_wa_id,
                    "❌ No se encontró información en el SAT para ese RFC / idCIF.\n"
                    "Verifica que estén bien escritos e intenta de nuevo."
                )
                return "OK", 200
            print("Error SAT (ValueError):", e)
            wa_send_text(from_wa_id, "❌ Error consultando SAT. Intenta de nuevo.")
            return "OK", 200
        except Exception as e:
            print("Error SAT:", e)
            wa_send_text(from_wa_id, "❌ Error consultando SAT. Intenta de nuevo.")
            return "OK", 200

        base_dir = os.path.dirname(os.path.abspath(__file__))

        regimen = (datos.get("REGIMEN") or "").strip()
        if regimen == "Régimen de Sueldos y Salarios e Ingresos Asimilados a Salarios":
            nombre_plantilla = "plantilla-asalariado.docx"
        else:
            nombre_plantilla = "plantilla.docx"

        ruta_plantilla = os.path.join(base_dir, nombre_plantilla)

        # Si el usuario pidió DOCX extra
        t_upper = (text_body or "").upper()
        quiere_docx = ("DOCX" in t_upper) or ("WORD" in t_upper) or ("AMBOS" in t_upper)

        # 4) Generar, convertir y enviar DENTRO del tempdir
        with tempfile.TemporaryDirectory() as tmpdir:
            nombre_base = datos.get("CURP") or rfc or "CONSTANCIA"
            nombre_docx = f"{nombre_base}_RFC.docx"
            ruta_docx = os.path.join(tmpdir, nombre_docx)

            reemplazar_en_documento(ruta_plantilla, ruta_docx, datos)

            # leer docx bytes
            with open(ruta_docx, "rb") as f:
                docx_bytes = f.read()

            # intentar PDF (default)
            try:
                pdf_path = os.path.join(tmpdir, os.path.splitext(nombre_docx)[0] + ".pdf")

                docx_to_pdf_aspose(
                    docx_path=ruta_docx,
                    pdf_path=pdf_path
                )
                
                with open(pdf_path, "rb") as f:
                    pdf_bytes = f.read()

                pdf_filename = os.path.splitext(nombre_docx)[0] + ".pdf"

                media_pdf = wa_upload_document(
                    file_bytes=pdf_bytes,
                    filename=pdf_filename,
                    mime="application/pdf"
                )

                wa_send_document(
                    to_wa_id=from_wa_id,
                    media_id=media_pdf,
                    filename=pdf_filename,
                    caption="✅ Aquí está tu constancia en PDF."
                )

                # opcional docx
                if quiere_docx:
                    media_docx = wa_upload_document(
                        file_bytes=docx_bytes,
                        filename=nombre_docx,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
                    wa_send_document(
                        to_wa_id=from_wa_id,
                        media_id=media_docx,
                        filename=nombre_docx,
                        caption="📄 (Opcional) También te dejo el archivo Word (DOCX)."
                    )

            except Exception as e:
                # fallback DOCX si PDF falla
                print("Error PDF/WhatsApp:", e)
                try:
                    media_docx = wa_upload_document(
                        file_bytes=docx_bytes,
                        filename=nombre_docx,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )
                    wa_send_document(
                        to_wa_id=from_wa_id,
                        media_id=media_docx,
                        filename=nombre_docx,
                        caption="⚠️ No pude convertir a PDF, pero aquí está en DOCX."
                    )
                except Exception as e2:
                    print("Error enviando DOCX fallback:", e2)
                    wa_send_text(from_wa_id, "⚠️ Se generó, pero no pude enviar el archivo. Intenta de nuevo.")
                    return "OK", 200

        return "OK", 200

    except Exception as e:
        print("Error WA webhook:", e)
        return "OK", 200

@app.route("/", methods=["GET"])
def home():
    return "Backend OK. Usa POST /login y /generar desde el formulario."

@app.route("/login", methods=["POST"])
def login():
    """
    Body JSON:
    {
      "username": "cliente_demo",
      "password": "demo1234"
    }
    """
    data = request.get_json() or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"ok": False, "message": "Faltan usuario o contraseña."}), 400

    password_hash = USERS.get(username)
    if not password_hash or not check_password_hash(password_hash, password):
        return jsonify({"ok": False, "message": "Usuario o contraseña incorrectos."}), 401

    # ========= 1) IP + User-Agent =========
    ip = get_client_ip()
    ua = request.headers.get("User-Agent", "desconocido")

    evento = {
        "ip": ip,
        "fecha": datetime.now(ZoneInfo("America/Mexico_City")).isoformat(),
        "ua": ua,
    }
    HISTORIAL_LOGIN.setdefault(username, []).append(evento)

    # ========= 2) Control por IP (opcional) =========
    info_ip = USERS_IP_INFO.get(username)
    if info_ip:
        if info_ip.get("bloquear_otras") and info_ip.get("ip") and info_ip["ip"] != ip:
            return jsonify({
                "ok": False,
                "message": (
                    "Este usuario ya se encuentra registrado con otra dirección IP "
                    f"({info_ip['ip']}). No se permite iniciar sesión desde una IP distinta."
                ),
            }), 403
    else:
        # primera vez: registramos IP (si quieres bloquear luego, ya la tienes)
        USERS_IP_INFO[username] = {
            "ip": ip,
            "bloquear_otras": BLOQUEAR_IP_POR_DEFAULT,
        }

    # ========= 3) Solo 1 sesión por usuario =========
    if username in ACTIVE_SESSIONS:
        return jsonify({
            "ok": False,
            "message": "Este usuario ya tiene una sesión activa en otro dispositivo. "
                       "Cierra sesión ahí para poder entrar aquí."
        }), 409

    # ========= 4) Crear token =========
    token = crear_sesion(username)

    resp = jsonify({"ok": True, "token": token, "message": "Login correcto."})
    resp.headers["Access-Control-Expose-Headers"] = "Authorization"
    return resp

@app.route("/logout", methods=["POST"])
def logout():
    user = usuario_actual_o_none()
    if not user:
        return jsonify({"ok": True})
    token = ACTIVE_SESSIONS.pop(user, None)
    if token:
        TOKEN_TO_USER.pop(token, None)
    return jsonify({"ok": True})

@app.route("/generar", methods=["POST"])
def generar_constancia():
    global REQUEST_TOTAL, REQUEST_POR_DIA, SUCCESS_COUNT, SUCCESS_RFCS

    # ------- AUTENTICACIÓN -------
    user = usuario_actual_o_none()
    if not user:
        return jsonify({
            "ok": False,
            "message": "No autorizado. Inicia sesión primero."
        }), 401

    # ====== STATS: request ======
    def _inc_req(s):
        # total + por día
        from stats_store import inc_request, inc_user_request
        inc_request(s)
        inc_user_request(s, user)
    get_and_update(STATS_PATH, _inc_req)
    
    # ------- CONTROL LÍMITE DIARIO POR USUARIO -------
    hoy_str = hoy_mexico().isoformat()
    info = USO_POR_USUARIO.get(user)
    if not info or info.get("hoy") != hoy_str:
        info = {"hoy": hoy_str, "count": 0}
        USO_POR_USUARIO[user] = info

    if info["count"] >= LIMITE_DIARIO:
        return jsonify({
            "ok": False,
            "message": "Has alcanzado el límite diario de constancias para esta cuenta."
        }), 429

    info["count"] += 1
    # ----------------------------------------------

    REQUEST_TOTAL += 1
    REQUEST_POR_DIA[hoy_str] = REQUEST_POR_DIA.get(hoy_str, 0) + 1

    print(
        f"[{datetime.utcnow().isoformat()}] Solicitud #{REQUEST_TOTAL} a /generar "
        f"(hoy: {REQUEST_POR_DIA[hoy_str]}) por usuario: {user}"
    )

    rfc = (request.form.get("rfc") or "").strip().upper()
    idcif = (request.form.get("idcif") or "").strip()
    lugar_emision = (request.form.get("lugar_emision") or "").strip()

    if not rfc or not idcif:
        return jsonify({
            "ok": False,
            "message": "Falta RFC o idCIF."
        }), 400

    try:
        datos = extraer_datos_desde_sat(rfc, idcif)
    except ValueError as e:
        if str(e) == "SIN_DATOS_SAT":
            return jsonify({
                "ok": False,
                "message": (
                    "No se encontró información en el SAT para ese RFC / idCIF. "
                    "Verifica que estén bien escritos o que el contribuyente esté dado de alta."
                )
            }), 404
        print("Error consultando SAT (datos no válidos):", e)
        return jsonify({"ok": False, "message": "Error consultando SAT o extrayendo datos."}), 500
    except Exception as e:
        print("Error consultando SAT:", e)
        return jsonify({"ok": False, "message": "Error consultando SAT o extrayendo datos."}), 500

    if lugar_emision:
        hoy = hoy_mexico()
        dia = f"{hoy.day:02d}"
        mes = MESES_ES[hoy.month]
        anio = hoy.year
        datos["FECHA"] = f"{lugar_emision.upper()} A {dia} DE {mes} DE {anio}"

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Elegir plantilla según el régimen
    regimen = (datos.get("REGIMEN") or "").strip()

    if regimen == "Régimen de Sueldos y Salarios e Ingresos Asimilados a Salarios":
        nombre_plantilla = "plantilla-asalariado.docx"   # << el archivo especial
    else:
        nombre_plantilla = "plantilla.docx"             # << la plantilla normal

    ruta_plantilla = os.path.join(base_dir, nombre_plantilla)

    with tempfile.TemporaryDirectory() as tmpdir:
        nombre_base = datos.get("CURP") or rfc or "CONSTANCIA"
        nombre_docx = f"{nombre_base}_RFC.docx"
        ruta_docx = os.path.join(tmpdir, nombre_docx)

        reemplazar_en_documento(ruta_plantilla, ruta_docx, datos)

        # ====== STATS: success ======
        def _inc_ok(s):
            from stats_store import inc_success
            inc_success(s, user, rfc)
        get_and_update(STATS_PATH, _inc_ok)

        print(f"[OK] Constancia #{SUCCESS_COUNT} generada correctamente para RFC: {rfc}")

        response = send_file(
            ruta_docx,
            mimetype=(
                "application/"
                "vnd.openxmlformats-officedocument.wordprocessingml.document"
            ),
            as_attachment=True,
            download_name=nombre_docx,
        )

        response.headers["Access-Control-Expose-Headers"] = "Content-Disposition"
        return response

@app.route("/stats", methods=["GET"])
def stats():
    # opcional: proteger con token
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    return jsonify({
        "total_solicitudes": s.get("request_total", 0),
        "total_ok": s.get("success_total", 0),
        "por_dia": s.get("por_dia", {}),
        "por_usuario": s.get("por_usuario", {}),
        "ultimos_rfcs_ok": s.get("last_success", []),
        "stats_path": STATS_PATH,
    })

@app.route("/admin/logins", methods=["GET"])
def admin_logins():
    """
    Historial de logins por usuario (IP, fecha, navegador).
    """
    return jsonify(HISTORIAL_LOGIN)

@app.route("/admin", methods=["GET"])
def admin_panel():
    # opcional: proteger
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return "Forbidden", 403

    s = get_state(STATS_PATH)
    total = s.get("request_total", 0)
    ok = s.get("success_total", 0)
    por_dia = s.get("por_dia", {})
    por_usuario = s.get("por_usuario", {})
    last_rfcs = s.get("last_success", [])[-20:][::-1]

    # ordena días recientes
    days_sorted = sorted(por_dia.items(), key=lambda x: x[0], reverse=True)[:14]

    # ordena usuarios por count hoy
    usuarios_list = []
    for u, info in por_usuario.items():
        usuarios_list.append((u, info.get("hoy"), int(info.get("count") or 0), int(info.get("success") or 0)))
    usuarios_list.sort(key=lambda x: (x[1], x[2]), reverse=True)

    html_days = "".join(
        f"<tr><td>{d}</td><td>{v.get('requests',0)}</td><td>{v.get('success',0)}</td></tr>"
        for d, v in days_sorted
    )

    html_users = "".join(
        f"<tr><td>{u}</td><td>{hoy}</td><td>{cnt}</td><td>{succ}</td></tr>"
        for u, hoy, cnt, succ in usuarios_list[:40]
    )

    html_rfcs = "".join(f"<li>{r}</li>" for r in last_rfcs)

    return f"""
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Admin - Stats</title>
  <style>
    body{{font-family:Arial,sans-serif;background:#f6f7fb;margin:0;padding:24px;color:#111}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}}
    .card{{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:14px;box-shadow:0 1px 6px rgba(0,0,0,.04)}}
    h1{{margin:0 0 14px}}
    h2{{margin:0 0 10px;font-size:16px}}
    .big{{font-size:28px;font-weight:700}}
    table{{width:100%;border-collapse:collapse}}
    td,th{{padding:8px;border-top:1px solid #eee;text-align:left;font-size:14px}}
    .muted{{color:#6b7280}}
    ul{{margin:0;padding-left:18px}}
    .row{{display:flex;gap:10px;align-items:baseline}}
    .pill{{display:inline-block;background:#eef2ff;border:1px solid #e0e7ff;color:#3730a3;padding:2px 8px;border-radius:999px;font-size:12px}}
  </style>
</head>
<body>
  <h1>📊 Estadísticas</h1>

  <div class="grid">
    <div class="card">
      <h2>Total solicitudes</h2>
      <div class="big">{total}</div>
      <div class="muted">Incluye intentos fallidos.</div>
    </div>
    <div class="card">
      <h2>Total constancias OK</h2>
      <div class="big">{ok}</div>
      <div class="muted">Generadas correctamente.</div>
    </div>
    <div class="card">
      <h2>Archivo</h2>
      <div class="row"><span class="pill">STATS_PATH</span><span class="muted">{STATS_PATH}</span></div>
      <div class="muted" style="margin-top:8px">Si no tienes Disk en Render, esto se borra al reiniciar.</div>
    </div>
  </div>

  <div class="grid" style="margin-top:14px">
    <div class="card">
      <h2>Últimos 14 días</h2>
      <table>
        <thead><tr><th>Día</th><th>Requests</th><th>OK</th></tr></thead>
        <tbody>{html_days}</tbody>
      </table>
    </div>

    <div class="card">
      <h2>Uso por usuario (hoy)</h2>
      <table>
        <thead><tr><th>Usuario</th><th>Día</th><th>Count</th><th>OK</th></tr></thead>
        <tbody>{html_users}</tbody>
      </table>
    </div>

    <div class="card">
      <h2>Últimos RFC OK</h2>
      <ul>{html_rfcs}</ul>
    </div>
  </div>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(debug=True, port=5000)


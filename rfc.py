# -*- coding: utf-8 -*-
import os
import sys
import re
import ssl
import tempfile
import json
import jwt
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
from datetime import datetime, timedelta

from PIL import Image
import numpy as np
import cv2
import pytesseract
from io import BytesIO
import urllib.parse

from collections import deque

import threading

try:
    from pyzbar.pyzbar import decode as zbar_decode
    PYZBAR_OK = True
except Exception as e:
    print("pyzbar disabled:", e)
    PYZBAR_OK = False
    zbar_decode = None

WA_PROCESSED_MSG_IDS = set()
WA_PROCESSED_QUEUE = deque(maxlen=2000)
WA_LOCK = threading.Lock()

TEST_NUMBERS = set(x.strip() for x in (os.getenv("TEST_NUMBERS", "") or "").split(",") if x.strip())
PRICE_PER_OK_MXN = int(os.getenv("PRICE_PER_OK_MXN", "0") or "0")

def is_test_request(user_key: str, text_body: str = "") -> bool:
    if user_key in TEST_NUMBERS:
        return True
    t = (text_body or "").upper()
    return ("PRUEBA" in t) or ("TEST" in t)

def wa_seen_msg(msg_id: str) -> bool:
    if not msg_id:
        return False
    with WA_LOCK:
        if msg_id in WA_PROCESSED_MSG_IDS:
            return True
        WA_PROCESSED_MSG_IDS.add(msg_id)
        WA_PROCESSED_QUEUE.append(msg_id)
        if len(WA_PROCESSED_MSG_IDS) > 2500:
            WA_PROCESSED_MSG_IDS.clear()
            WA_PROCESSED_MSG_IDS.update(list(WA_PROCESSED_QUEUE))
    return False

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
    "gerardo.calzada.oficina": generate_password_hash("CalzadaIDCIF26"),
    # "papeleria_lupita": generate_password_hash("clave_lupita"),
    # "abogados_lopez": generate_password_hash("clave_lopez"),
}

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

# ================== JWT AUTH (PRO) ==================
JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_DAYS = int(os.getenv("JWT_DAYS", "30") or "30")
ALLOW_KICKOUT = os.getenv("ALLOW_KICKOUT", "0") in ("1", "true", "TRUE", "yes", "YES")

# Guardamos "sesión actual" (jti) por usuario en /data para que sobreviva reinicios
SESSIONS_PATH = os.getenv("SESSIONS_PATH", "/data/sessions.json")

def _read_json_file(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except FileNotFoundError:
        return {}
    except Exception as e:
        print("WARN read sessions json:", e)
        return {}

def _atomic_write_json(path: str, data: dict):
    # write seguro (verás esto mucho en Render)
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def get_sessions_state() -> dict:
    state = _read_json_file(SESSIONS_PATH)
    if "user_session" not in state:
        # { "username": {"jti": "...", "exp": 1234567890} }
        state["user_session"] = {}
    return state

def set_user_session(username: str, jti: str | None, exp_ts: int | None):
    st = get_sessions_state()
    if jti and exp_ts:
        st["user_session"][username] = {"jti": jti, "exp": int(exp_ts)}
    else:
        st["user_session"].pop(username, None)
    _atomic_write_json(SESSIONS_PATH, st)

def get_user_session(username: str) -> dict | None:
    st = get_sessions_state()
    return (st.get("user_session") or {}).get(username)

def get_user_jti(username: str) -> str | None:
    sess = get_user_session(username) or {}
    return sess.get("jti")

def is_user_session_expired(username: str) -> bool:
    sess = get_user_session(username)
    if not sess:
        return True
    exp_ts = sess.get("exp")
    if not exp_ts:
        return True
    return int(exp_ts) <= int(datetime.utcnow().timestamp())

def crear_jwt(username: str) -> str:
    if not JWT_SECRET:
        raise RuntimeError("Falta JWT_SECRET en variables de entorno.")

    jti = secrets.token_urlsafe(16)

    now = datetime.utcnow()
    exp = now + timedelta(days=JWT_DAYS)

    payload = {
        "sub": username,
        "jti": jti,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }

    # guardamos la sesión actual (jti + exp)
    set_user_session(username, jti, payload["exp"])

    token = jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return token

def verificar_jwt(token: str) -> str | None:
    if not token:
        return None
    if not JWT_SECRET:
        return None

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except Exception:
        return None

    username = payload.get("sub")
    jti = payload.get("jti")
    if not username or not jti:
        return None

    # "una sola sesión": el jti debe coincidir con el guardado en /data
    current = get_user_jti(username)
    if not current or current != jti:
        return None

    return username

def usuario_actual_o_none():
    auth_header = request.headers.get("Authorization", "") or ""
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.split(" ", 1)[1].strip()
    return verificar_jwt(token)

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

def wa_get_media_url(media_id: str) -> str:
    """
    1) Pide a Meta la URL temporal del media (image/audio/doc).
    """
    url = wa_api_url(f"{media_id}")
    headers = {"Authorization": f"Bearer {WA_TOKEN}"}
    r = requests.get(url, headers=headers, timeout=30)
    if not r.ok:
        print("WA GET MEDIA URL ERROR:", r.status_code, r.text)
    r.raise_for_status()
    data = r.json()
    return data.get("url")  # URL temporal

def wa_download_media_bytes(media_url: str) -> bytes:
    """
    2) Descarga el archivo desde la URL temporal.
    """
    headers = {"Authorization": f"Bearer {WA_TOKEN}"}
    r = requests.get(media_url, headers=headers, timeout=60)
    if not r.ok:
        print("WA DOWNLOAD MEDIA ERROR:", r.status_code, r.text[:4000])
    r.raise_for_status()
    return r.content

def _img_bytes_to_cv2(img_bytes: bytes):
    """
    Convierte bytes de imagen a matriz OpenCV (BGR).
    """
    arr = np.frombuffer(img_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    return img

def decode_qr_from_image_bytes(img_bytes: bytes) -> list[str]:
    """
    QR robusto SOLO con OpenCV (sin pyzbar/zbar):
    - detectAndDecodeMulti
    - reescalado
    - binarización adaptativa
    """
    img = _img_bytes_to_cv2(img_bytes)
    if img is None:
        return []

    def _try_opencv_qr(bgr):
        try:
            det = cv2.QRCodeDetector()
            ok, decoded, _, _ = det.detectAndDecodeMulti(bgr)
            if ok and decoded:
                return [d for d in decoded if d]
            d, _, _ = det.detectAndDecode(bgr)
            return [d] if d else []
        except Exception:
            return []

    # 1) directo
    outs = _try_opencv_qr(img)
    if outs:
        return list(dict.fromkeys(outs))

    # 2) reescalados
    h, w = img.shape[:2]
    for scale in (1.5, 2.0, 2.5, 3.0):
        rs = cv2.resize(img, (int(w*scale), int(h*scale)), interpolation=cv2.INTER_CUBIC)
        outs = _try_opencv_qr(rs)
        if outs:
            return list(dict.fromkeys(outs))

    # 3) binarización
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                cv2.THRESH_BINARY, 31, 7)
    thr_bgr = cv2.cvtColor(thr, cv2.COLOR_GRAY2BGR)
    outs = _try_opencv_qr(thr_bgr)
    return list(dict.fromkeys([o for o in outs if o]))

def parse_sat_qr_text_to_rfc_idcif(qr_text: str):
    """
    Si el QR trae URL del SAT tipo:
    ...validadorqr.jsf?D1=10&D2=1&D3=IDCIF_RFC
    Extrae RFC + IDCIF.
    """
    if not qr_text:
        return (None, None)

    t = qr_text.strip()

    # Si viene URL
    if "D3=" in t:
        try:
            parsed = urllib.parse.urlparse(t)
            qs = urllib.parse.parse_qs(parsed.query)
            d3 = (qs.get("D3") or [None])[0]
            if d3 and "_" in d3:
                idcif, rfc = d3.split("_", 1)
                rfc = (rfc or "").strip().upper()
                idcif = (idcif or "").strip()
                if rfc and idcif:
                    return (rfc, idcif)
        except Exception:
            pass

    # A veces el QR no trae URL, trae texto con RFC/IDCIF
    rfc, idcif = extraer_rfc_idcif(t)
    return (rfc, idcif)

def ocr_text_from_image_bytes(img_bytes: bytes, timeout_sec: int = 2) -> str:
    """
    OCR con timeout corto para NO tumbar gunicorn.
    Si tarda más, lo cortamos y regresamos vacío.
    """
    img = _img_bytes_to_cv2(img_bytes)
    if img is None:
        return ""

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.bilateralFilter(gray, 9, 75, 75)
    thr = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                cv2.THRESH_BINARY, 31, 8)

    config = "--oem 1 --psm 6"

    try:
        # pytesseract soporta timeout=
        text = pytesseract.image_to_string(thr, lang="spa+eng", config=config, timeout=timeout_sec)
    except Exception:
        try:
            text = pytesseract.image_to_string(thr, config=config, timeout=timeout_sec)
        except Exception:
            return ""

    return (text or "").strip()

def extract_rfc_idcif_from_image_bytes(img_bytes: bytes):
    """
    1) QR robusto
    2) OCR opcional con timeout corto (controlado por env OCR_ENABLED)
    """
    # 1) QR
    qr_list = decode_qr_from_image_bytes(img_bytes)
    for qr in qr_list:
        rfc, idcif = parse_sat_qr_text_to_rfc_idcif(qr)
        if rfc and idcif:
            return rfc, idcif, "QR"

    # 2) OCR solo si lo habilitas
    OCR_ENABLED = os.getenv("OCR_ENABLED", "0") in ("1", "true", "TRUE", "yes", "YES")
    if not OCR_ENABLED:
        return None, None, "NO_QR"

    text = ocr_text_from_image_bytes(img_bytes, timeout_sec=2)
    rfc, idcif = extraer_rfc_idcif(text)
    if rfc and idcif:
        return rfc, idcif, "OCR"

    return rfc, idcif, "NONE"

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

# ================== DETECCIÓN INPUT TYPE + CURP ==================

CURP_REGEX = r"\b([A-Z][AEIOUX][A-Z]{2}\d{6}[HM][A-Z]{5}[A-Z0-9]\d)\b"

def extraer_curp(texto: str):
    if not texto:
        return None
    t = (texto or "").strip().upper()
    m = re.search(CURP_REGEX, t)
    return m.group(1) if m else None

def detect_input_type(text_body: str, had_image: bool, fuente_img: str = "") -> str:
    """
    Prioridad:
      - si viene de imagen y se detectó por QR/OCR => QR
      - si el texto contiene CURP => CURP
      - si contiene RFC+IDCIF => RFC_IDCIF
      - default => RFC_IDCIF
    """
    if had_image and (fuente_img in ("QR", "OCR")):
        return "QR"

    curp = extraer_curp(text_body or "")
    if curp:
        return "CURP"

    rfc, idcif = extraer_rfc_idcif(text_body or "")
    if rfc and idcif:
        return "RFC_IDCIF"

    return "RFC_IDCIF"

def make_ok_key(input_type: str, rfc: str | None, curp: str | None) -> str:
    input_type = (input_type or "").upper().strip()
    if input_type == "CURP":
        return f"CURP:{(curp or '').upper().strip()}"
    # RFC_IDCIF o QR => dedupe por RFC
    return f"RFC:{(rfc or '').upper().strip()}"

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
        if wa_seen_msg(msg_id):
            print("WA DUPLICATE msg_id ignored:", msg_id)
            return "OK", 200

        contacts = value.get("contacts") or []

        raw_wa_id = (contacts[0].get("wa_id") if contacts else None) or msg.get("from")
        from_wa_id = normalizar_wa_to(raw_wa_id)
        print("WA TO normalized:", raw_wa_id, "->", from_wa_id)

        # 🔒 ALLOWLIST (lista blanca)
        st = get_state(STATS_PATH)
        
        try:
            from stats_store import is_allowed
            if not is_allowed(st, from_wa_id):
                # Recomendación: ignorar silenciosamente (no dar pistas)
                print("WA NOT ALLOWED (ignored):", from_wa_id)
                return "OK", 200
        
                # Si prefieres avisar:
                # wa_send_text(from_wa_id, "⛔ No autorizado. Contacta al administrador.")
                # return "OK", 200
        except Exception as e:
            print("Allowlist check error:", e)
            # si falla, por seguridad puedes bloquear o dejar pasar; yo dejaría pasar:
            # return "OK", 200

        # 🔒 Bloqueo por WA
        def _is_blocked(s):
            from stats_store import is_blocked
            return is_blocked(s, from_wa_id)
        
        st = get_state(STATS_PATH)
        if (st.get("blocked_users") or {}).get(from_wa_id):
            # opcional: responder algo corto o ni responder
            wa_send_text(from_wa_id, "⛔ Tu número está suspendido. Contacta al administrador.")
            return "OK", 200
        
        msg_type = msg.get("type")

        text_body = ""
        image_bytes = None
        
        if msg_type == "text":
            text_body = ((msg.get("text") or {}).get("body") or "").strip()
        
        elif msg_type == "image":
            # WhatsApp manda un media_id en msg["image"]["id"]
            media_id = ((msg.get("image") or {}).get("id") or "").strip()
            if media_id:
                try:
                    media_url = wa_get_media_url(media_id)
                    image_bytes = wa_download_media_bytes(media_url)
                except Exception as e:
                    print("Error descargando imagen:", e)
        
        elif msg_type == "document":
            # A veces mandan screenshot como "document"
            media_id = ((msg.get("document") or {}).get("id") or "").strip()
            mime = ((msg.get("document") or {}).get("mime_type") or "")
            if media_id and (mime.startswith("image/") or mime in ("application/octet-stream", "")):
                try:
                    media_url = wa_get_media_url(media_id)
                    image_bytes = wa_download_media_bytes(media_url)
                except Exception as e:
                    print("Error descargando documento/imagen:", e)

        if not from_wa_id:
            return "OK", 200

        # Si viene imagen, intentamos extraer RFC+IDCIF de la foto
        fuente_img = ""
        if image_bytes:
            rfc_img, idcif_img, fuente_img = extract_rfc_idcif_from_image_bytes(image_bytes)
        
            if rfc_img and idcif_img:
                # simulamos como si hubiera escrito texto
                text_body = f"RFC: {rfc_img} IDCIF: {idcif_img}"
                wa_send_text(
                    from_wa_id,
                    f"✅ Detecté datos por {fuente_img}.\n{rfc_img} {idcif_img}\n"
                )
            else:
                wa_send_text(
                    from_wa_id,
                    "📸 Recibí tu imagen, pero no pude detectar el QR o el texto.\n\n"
                    "Tips:\n"
                    "• Manda la foto del QR lo más centrada posible\n"
                    "• Sin reflejos y con buena luz\n"
                    "• O escribe: RFC IDCIF\n\n"
                    "Ejemplo:\nTOHJ640426XXX 19010347XXX"
                )
                return "OK", 200
        
        # Si NO hay texto y NO hay imagen válida
        if not text_body:
            wa_send_text(
                from_wa_id,
                "📩 Envíame RFC e idCIF o una foto donde se vea el QR.\n\n"
                "Ejemplo texto:\nTOHJ640426XXX 19010347XXX"
            )
            return "OK", 200

        # ✅ Detectar tipo de entrada (CURP vs RFC_IDCIF vs QR)
        input_type = detect_input_type(text_body, had_image=bool(image_bytes), fuente_img=(fuente_img or ""))
        
        curp_in = extraer_curp(text_body) if input_type == "CURP" else None
        
        # 1) Extraer RFC + idCIF (si aplica)
        rfc, idcif = extraer_rfc_idcif(text_body)

        if input_type == "CURP":
            # Si todavía no implementas generar por CURP, aquí solo detectamos y cobramos distinto cuando exista ese flujo.
            wa_send_text(
                from_wa_id,
                "✅ Detecté que enviaste una CURP.\n\n"
                "Para generar la constancia por ahora envíame:\n"
                "RFC + IDCIF\n\n"
                "Ejemplo:\nTOHJ640426XXX 19010347XXX\n\n"
                "O envía foto donde se vea el QR."
            )
            # si quieres contar como request, déjalo; si NO quieres, puedes regresar antes de inc_request.
            return "OK", 200

        if not rfc or not idcif:
            wa_send_text(
                from_wa_id,
                "✅ Recibí tu mensaje.\n\nAhora envíame los datos en este formato:\n"
                "RFC IDCIF\n\n"
                "Tip: si quieres también Word, escribe al final: DOCX"
            )
            return "OK", 200

        # ✅ STATS: contar SOLO solicitudes reales (cuando ya hay RFC + IDCIF)
        test_mode = is_test_request(from_wa_id, text_body)

        if not test_mode:
            def _inc_req_real(s):
                from stats_store import inc_request, inc_user_request
                inc_request(s)
                inc_user_request(s, from_wa_id)  # 👈 número real del cliente
        
            get_and_update(STATS_PATH, _inc_req_real)
        
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
                
                # caja para leer resultado fuera
                _bill_out = {"reason": None, "billed": False, "price": 0, "type": None, "key": None}

                def _ok_and_bill(s):
                    from stats_store import inc_success, bill_success_if_new, log_attempt, resolve_price
                
                    # precio por usuario + tipo (WA usa el número como user)
                    price_mxn = resolve_price(s, from_wa_id, input_type)
                
                    ok_key = make_ok_key(input_type, rfc, curp_in)
                
                    # ✅ dedupe global por ok_key, cobra con price_mxn
                    res = bill_success_if_new(
                        s,
                        user=from_wa_id,
                        ok_key=ok_key,
                        input_type=input_type,
                        price_mxn=price_mxn,
                        is_test=test_mode
                    )
                
                    _bill_out["reason"] = res.get("reason")
                    _bill_out["billed"] = bool(res.get("billed"))
                    _bill_out["price"] = int(res.get("price") or price_mxn or 0)
                    _bill_out["type"] = input_type
                    _bill_out["key"] = ok_key
                
                    if res.get("billed"):
                        inc_success(s, from_wa_id, rfc)  # success = cobrados (tu lógica actual)
                        log_attempt(s, from_wa_id, ok_key, True, "BILLED_OK", {"via": "WA", "type": input_type, "price": price_mxn}, is_test=test_mode)
                    else:
                        code = "OK_NO_BILL"
                        if res.get("reason") == "DUPLICATE":
                            code = "OK_DUPLICATE_NO_BILL"
                        elif res.get("reason") == "TEST":
                            code = "OK_TEST_NO_BILL"
                        log_attempt(s, from_wa_id, ok_key, True, code, {"via": "WA", "type": input_type, "reason": res.get("reason")}, is_test=test_mode)
                
                get_and_update(STATS_PATH, _ok_and_bill)
                
                if _bill_out["reason"] == "DUPLICATE":
                    wa_send_text(from_wa_id, "⚠️ Este trámite ya fue generado antes. No se cobrará de nuevo.")

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

                    _bill_out = {"reason": None, "billed": False}

                    def _ok_and_bill(s):
                        from stats_store import inc_success, bill_success_if_new, log_attempt, set_price
                    
                        set_price(s, PRICE_PER_OK_MXN)
                    
                        res = bill_success_if_new(s, from_wa_id, rfc, is_test=test_mode)
                        _bill_out["reason"] = res.get("reason")
                        _bill_out["billed"] = bool(res.get("billed"))
                    
                        if res["billed"]:
                            inc_success(s, from_wa_id, rfc)  # ✅ success = cobrados
                            log_attempt(s, from_wa_id, rfc, True, "BILLED_OK", {"via": "WA"})
                        else:
                            if res["reason"] == "DUPLICATE":
                                log_attempt(s, from_wa_id, rfc, True, "OK_DUPLICATE_NO_BILL", {"via": "WA"})
                            elif res["reason"] == "TEST":
                                log_attempt(s, from_wa_id, rfc, True, "OK_TEST_NO_BILL", {"via": "WA"})
                            else:
                                log_attempt(s, from_wa_id, rfc, True, "OK_NO_BILL", {"via": "WA", "reason": res["reason"]})
                    
                    get_and_update(STATS_PATH, _ok_and_bill)
                    
                    if _bill_out["reason"] == "DUPLICATE":
                        wa_send_text(from_wa_id, "⚠️ Este RFC ya fue generado antes. No se cobrará de nuevo.")

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
        USERS_IP_INFO[username] = {"ip": ip, "bloquear_otras": BLOQUEAR_IP_POR_DEFAULT}

    # ========= 3) Solo 1 sesión por usuario (persistente) =========
    sess = get_user_session(username)
    now_ts = int(datetime.utcnow().timestamp())
    
    # si existe sesión y NO ha expirado → bloquear login
    if sess and int(sess.get("exp") or 0) > now_ts and not ALLOW_KICKOUT:
        return jsonify({
            "ok": False,
            "message": "Este usuario ya tiene una sesión activa en otro dispositivo."
        }), 409
    
    # si existe pero ya expiró → limpiar
    if sess and int(sess.get("exp") or 0) <= now_ts:
        set_user_session(username, None, None)

    # ========= 4) Crear JWT =========
    try:
        token = crear_jwt(username)
    except Exception as e:
        print("JWT error:", e)
        return jsonify({"ok": False, "message": "Error creando sesión."}), 500

    resp = jsonify({"ok": True, "token": token, "message": "Login correcto."})
    resp.headers["Access-Control-Expose-Headers"] = "Authorization"
    return resp

@app.route("/logout", methods=["POST"])
def logout():
    user = usuario_actual_o_none()
    if not user:
        return jsonify({"ok": True})
    set_user_session(user, None, None)
    return jsonify({"ok": True})

@app.route("/generar", methods=["POST"])
def generar_constancia():
    global REQUEST_TOTAL, REQUEST_POR_DIA, SUCCESS_COUNT, SUCCESS_RFCS

    # ------- AUTENTICACIÓN -------
    user = usuario_actual_o_none()
    if not user:
        return jsonify({"ok": False, "message": "No autorizado"}), 401

    # ====== TEST MODE (WEB) ======
    # En web normalmente no hay texto, así que solo depende del user
    test_mode = is_test_request(user, "")

    def _set_price(s):
        # set_price ya está en stats_store.py
        s.setdefault("billing", {})
        s["billing"]["price_mxn"] = int(PRICE_PER_OK_MXN or 0)
        
    get_and_update(STATS_PATH, _set_price)
    
    # ====== STATS: request (SOLO si NO es prueba) ======
    if not test_mode:
        def _inc_req(s):
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

        # ====== STATS: success (solo si se cobró y no es duplicate/test) ======
        def _inc_ok(s):
            from stats_store import bill_success_if_new, log_attempt, resolve_price, inc_success
        
            input_type = "RFC_IDCIF"
            price_mxn = resolve_price(s, user, input_type)
        
            ok_key = make_ok_key(input_type, rfc, None)
        
            res = bill_success_if_new(
                s,
                user=user,
                ok_key=ok_key,
                input_type=input_type,
                price_mxn=price_mxn,
                is_test=test_mode
            )
        
            if res.get("billed"):
                inc_success(s, user, rfc)
                log_attempt(s, user, ok_key, True, "BILLED_OK", {"via": "WEB", "type": input_type, "price": price_mxn}, is_test=test_mode)
            else:
                code = "OK_NO_BILL"
                if res.get("reason") == "DUPLICATE":
                    code = "OK_DUPLICATE_NO_BILL"
                elif res.get("reason") == "TEST":
                    code = "OK_TEST_NO_BILL"
                log_attempt(s, user, ok_key, True, code, {"via": "WEB", "type": input_type, "reason": res.get("reason")}, is_test=test_mode)
        
        get_and_update(STATS_PATH, _inc_ok)

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

@app.route("/admin/kick", methods=["POST"])
def admin_kick_user():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    if not isinstance(data, dict):
        return jsonify({"ok": False, "message": "JSON inválido"}), 400

    username = (data.get("username") or "").strip()
    if not username:
        return jsonify({"ok": False, "message": "Falta username"}), 400

    set_user_session(username, None, None)
    return jsonify({"ok": True, "message": f"Sesión cerrada para {username}"})

@app.route("/admin/okrfcs/<path:user_key>", methods=["GET"])
def admin_ok_rfcs_user(user_key):
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    pu = (s.get("por_usuario") or {}).get(user_key) or {}
    # en tu stats_store normalmente guardas algo como last_success
    ok_rfcs = pu.get("last_success") or pu.get("rfcs_ok") or []

    # devuelve más nuevos primero
    ok_rfcs = list(ok_rfcs)[-100:][::-1]

    return jsonify({
        "ok": True,
        "user": user_key,
        "hoy": pu.get("hoy"),
        "count": pu.get("count", 0),
        "success": pu.get("success", 0),
        "ok_rfcs": ok_rfcs,
    })

@app.route("/admin/user/<path:user_key>", methods=["GET"])
def admin_user_html(user_key):
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return "Forbidden", 403

    s = get_state(STATS_PATH)
    pu = (s.get("por_usuario") or {}).get(user_key) or {}
    rfcs = (pu.get("rfcs_ok") or [])[-50:][::-1]

    rows = "".join(
        f"<tr><td>{i+1}</td><td>{r}</td></tr>"
        for i, r in enumerate(rfcs)
    ) or "<tr><td colspan='2'>Sin RFC OK</td></tr>"

    return f"""
    <h2>📱 Número: {user_key}</h2>
    <p>Solicitudes: {pu.get("count",0)} | OK: {pu.get("success",0)}</p>
    <table border=1 cellpadding=6>
      <tr><th>#</th><th>RFC generado correctamente</th></tr>
      {rows}
    </table>
    """

@app.route("/admin/billing", methods=["GET"])
def admin_billing():
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    b = s.get("billing") or {}
    return jsonify({
        "ok": True,
        "price_mxn": b.get("price_mxn", 0),
        "total_billed": b.get("total_billed", 0),
        "total_revenue_mxn": b.get("total_revenue_mxn", 0),
        "by_user": b.get("by_user", {}),
    })

@app.route("/admin/billing/user/<path:user_key>", methods=["GET"])
def admin_billing_user(user_key):
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    b = s.get("billing") or {}
    u = (b.get("by_user") or {}).get(user_key) or {}
    return jsonify({"ok": True, "user": user_key, "billing": u})

@app.route("/admin/wa/block", methods=["POST"])
def admin_wa_block():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = (data.get("wa_id") or "").strip()
    reason = (data.get("reason") or "").strip()
    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import block_user, log_attempt
        block_user(s, wa_id, reason=reason)
        log_attempt(s, wa_id, None, False, "USER_BLOCKED", {"reason": reason}, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "wa_id": wa_id, "blocked": True})

@app.route("/admin/wa/unblock", methods=["POST"])
def admin_wa_unblock():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = (data.get("wa_id") or "").strip()
    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import unblock_user, log_attempt
        unblock_user(s, wa_id)
        log_attempt(s, wa_id, None, True, "USER_UNBLOCKED", {}, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "wa_id": wa_id, "blocked": False})

@app.route("/admin/rfc/delete", methods=["POST"])
def admin_delete_rfc():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    rfc = (data.get("rfc") or "").strip().upper()
    if not rfc:
        return jsonify({"ok": False, "message": "Falta rfc"}), 400

    out = {"result": None}

    def _do(s):
        from stats_store import unbill_rfc, log_attempt
        res = unbill_rfc(s, rfc)
        out["result"] = res
        log_attempt(s, "ADMIN", rfc, True, "RFC_DELETED", res, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "rfc": rfc, "result": out["result"]})

@app.route("/admin/reset_all", methods=["POST"])
def admin_reset_all():
    # proteger con token admin
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    def _reset(state: dict):
        # estructura mínima “en blanco”
        state.clear()
        state.update({
            "request_total": 0,
            "success_total": 0,
            "por_dia": {},
            "por_usuario": {},
            "last_success": [],
            "attempts": {},
            "rfc_ok_index": {},   # dedupe de RFC OK
            "billing": {
                "price_mxn": float(PRICE_PER_OK_MXN or 0),
                "total_billed": 0,
                "total_revenue_mxn": 0.0,
                "by_user": {}
            }
        })

    get_and_update(STATS_PATH, _reset)
    return jsonify({"ok": True, "message": "Reset TOTAL aplicado (WA + WEB)"})

@app.route("/admin/wa/allow/add", methods=["POST"])
def admin_wa_allow_add():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = (data.get("wa_id") or "").strip()
    note = (data.get("note") or "").strip()

    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import allow_add, log_attempt
        allow_add(s, wa_id, note=note)
        log_attempt(s, wa_id, None, True, "ALLOW_ADDED", {"note": note}, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "wa_id": wa_id, "allowed": True})


@app.route("/admin/wa/allow/remove", methods=["POST"])
def admin_wa_allow_remove():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = (data.get("wa_id") or "").strip()
    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import allow_remove, log_attempt
        allow_remove(s, wa_id)
        log_attempt(s, wa_id, None, True, "ALLOW_REMOVED", {}, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "wa_id": wa_id, "allowed": False})


@app.route("/admin/wa/allow/enabled", methods=["POST"])
def admin_wa_allow_enabled():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    enabled = bool(data.get("enabled"))

    def _do(s):
        from stats_store import allow_set_enabled, log_attempt
        allow_set_enabled(s, enabled)
        log_attempt(s, "ADMIN", None, True, "ALLOWLIST_TOGGLE", {"enabled": enabled}, is_test=False)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "allowlist_enabled": enabled})


@app.route("/admin/wa/allow/list", methods=["GET"])
def admin_wa_allow_list():
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    return jsonify({
        "ok": True,
        "allowlist_enabled": bool(s.get("allowlist_enabled") or False),
        "allowlist_wa": s.get("allowlist_wa") or [],
        "allowlist_meta": s.get("allowlist_meta") or {},
    })

@app.route("/admin/pricing", methods=["GET"])
def admin_pricing_get():
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)
    return jsonify({"ok": True, "pricing": s.get("pricing") or {}})

@app.route("/admin/pricing/default", methods=["POST"])
def admin_pricing_set_default():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    input_type = (data.get("type") or "").strip().upper()
    price = int(data.get("price_mxn") or 0)

    out = {"ok": True}

    def _do(s):
        from stats_store import set_default_price
        set_default_price(s, input_type, price)

    try:
        get_and_update(STATS_PATH, _do)
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    return jsonify({"ok": True, "type": input_type, "price_mxn": price})

@app.route("/admin/pricing/user/set", methods=["POST"])
def admin_pricing_user_set():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    user = (data.get("user") or "").strip()
    input_type = (data.get("type") or "").strip().upper()
    price = int(data.get("price_mxn") or 0)

    def _do(s):
        from stats_store import set_user_price
        set_user_price(s, user, input_type, price)

    try:
        get_and_update(STATS_PATH, _do)
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)}), 400

    return jsonify({"ok": True, "user": user, "type": input_type, "price_mxn": price})

@app.route("/admin/pricing/user/delete", methods=["POST"])
def admin_pricing_user_delete():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    user = (data.get("user") or "").strip()
    input_type = (data.get("type") or "").strip().upper()  # opcional

    def _do(s):
        from stats_store import delete_user_price
        delete_user_price(s, user, input_type or None)

    get_and_update(STATS_PATH, _do)
    return jsonify({"ok": True, "user": user, "type": input_type or None})

@app.route("/admin", methods=["GET"])
def admin_panel():
    # opcional: proteger
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return "Forbidden", 403

    # ✅ SINCRONIZAR PRECIO DE RENTA
    def _set_price(s):
        from stats_store import set_price
        set_price(s, PRICE_PER_OK_MXN)

    get_and_update(STATS_PATH, _set_price)

    s = get_state(STATS_PATH)
    total = int(s.get("request_total", 0) or 0)
    ok = int(s.get("success_total", 0) or 0)
    por_dia = s.get("por_dia", {}) or {}
    por_usuario = s.get("por_usuario", {}) or {}
    last_rfcs = (s.get("last_success", []) or [])[-30:][::-1]

    ok_rate = (ok / total * 100.0) if total > 0 else 0.0

    # --- últimos 14 días ---
    days_sorted = sorted(por_dia.items(), key=lambda x: x[0], reverse=True)[:14]
    rows_days = []
    for d, v in days_sorted:
        req = int((v or {}).get("requests", 0) or 0)
        succ = int((v or {}).get("success", 0) or 0)
        rate = (succ / req * 100.0) if req > 0 else 0.0
        rows_days.append((d, req, succ, rate))

    html_days = "".join(
        f"""
        <tr>
          <td class="mono">{d}</td>
          <td class="num">{req}</td>
          <td class="num">{succ}</td>
          <td>
            <div class="bar">
              <div class="barFill" style="width:{rate:.1f}%"></div>
            </div>
            <div class="sub">{rate:.1f}%</div>
          </td>
        </tr>
        """
        for d, req, succ, rate in rows_days
    ) or """
        <tr><td colspan="4" class="empty">Sin datos aún.</td></tr>
    """

    # --- usuarios: orden por "hoy" y "count" ---
    usuarios_list = []
    for u, info in por_usuario.items():
        info = info or {}
        hoy = info.get("hoy") or ""
        cnt = int(info.get("count") or 0)
        succ = int(info.get("success") or 0)
        rate = (succ / cnt * 100.0) if cnt > 0 else (100.0 if succ > 0 else 0.0)
        usuarios_list.append((u, hoy, cnt, succ, rate))

    usuarios_list.sort(key=lambda x: (x[1], x[2], x[3]), reverse=True)

    html_users = "".join(
        f"""
        <tr>
          <td class="userCell">
            <div class="avatar">{(u[:1] or '?').upper()}</div>
            <div class="userMeta">
              <a
                  class="userName"
                  href="/admin/user/{u}?token={ADMIN_STATS_TOKEN}"
                  target="_blank"
                  style="color:inherit;text-decoration:underline"
                >
                  {u}
                </a>
              <div class="sub mono">{hoy or "—"}</div>
            </div>
          </td>
          <td class="num">{cnt}</td>
          <td class="num">{succ}</td>
          <td>
            <div class="bar">
              <div class="barFill" style="width:{rate:.1f}%"></div>
            </div>
            <div class="sub">{rate:.1f}%</div>
          </td>
        </tr>
        """
        for u, hoy, cnt, succ, rate in usuarios_list[:60]
    ) or """
        <tr><td colspan="4" class="empty">Sin usuarios aún.</td></tr>
    """

    html_rfcs = "".join(
        f'<span class="chip mono">{r}</span>'
        for r in last_rfcs if r
    ) or '<span class="muted">Sin RFC aún.</span>'

    hoy_top = usuarios_list[0][1] if usuarios_list and usuarios_list[0][1] else ""
    modo = "DISK" if (STATS_PATH or "").startswith("/data") else "TEMP"
    disk_hint = "Persistente (Render Disk)" if modo == "DISK" else "Se borra al reiniciar"

    dot_class = "ok" if modo == "DISK" else "warn"

    # ✅ HTML NORMAL (NO f-string) para que JS pueda usar {} sin romper Python
    html = r"""<!doctype html>
    <html lang="es">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width,initial-scale=1">
      <title>CSF Docs · Admin</title>
      <style>
        /* =====================
           VARIABLES
        ===================== */
        :root{
          --bg:#0b1020;
          --panel:rgba(255,255,255,.06);
          --panel2:rgba(255,255,255,.08);
          --border:rgba(255,255,255,.10);
          --text:#e8ecff;
          --muted:rgba(232,236,255,.70);
          --muted2:rgba(232,236,255,.55);
          --shadow:0 14px 40px rgba(0,0,0,.35);
          --radius:18px;
          --radius2:14px;
          --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
          --sans: system-ui, -apple-system, Segoe UI, Roboto, Arial;
          --ok:#22c55e;
          --warn:#f59e0b;
          --bad:#ef4444;
          --accent:#7c3aed;
          --accent2:#60a5fa;
        }
        
        /* =====================
           BASE
        ===================== */
        *{box-sizing:border-box}
        
        body{
          margin:0;
          font-family:var(--sans);
          background:
            radial-gradient(1200px 600px at 20% -10%, rgba(124,58,237,.35), transparent 60%),
            radial-gradient(900px 500px at 90% 0%, rgba(96,165,250,.25), transparent 55%),
            radial-gradient(900px 600px at 40% 110%, rgba(34,197,94,.12), transparent 55%),
            var(--bg);
          color:var(--text);
        }
        
        .wrap{
          max-width:1180px;
          margin:0 auto;
          padding:18px 16px 28px;
        }
        
        /* =====================
           TOPBAR
        ===================== */
        .topbar{
          position:sticky;
          top:0;
          z-index:5;
          backdrop-filter: blur(12px);
          background: linear-gradient(to bottom, rgba(11,16,32,.85), rgba(11,16,32,.55));
          border-bottom:1px solid rgba(255,255,255,.08);
        }
        
        .topbarInner{
          max-width:1180px;
          margin:0 auto;
          padding:14px 16px;
          display:flex;
          gap:14px;
          align-items:center;
          justify-content:space-between;
        }
        
        .brand{display:flex;gap:12px;align-items:center}
        
        .logo{
          width:40px;height:40px;border-radius:14px;
          background: linear-gradient(135deg, rgba(124,58,237,.95), rgba(96,165,250,.85));
          display:flex;align-items:center;justify-content:center;
          font-weight:900;
        }
        
        .title{display:flex;flex-direction:column;line-height:1.05}
        .title b{font-size:15px}
        .title span{font-size:12px;color:var(--muted)}
        
        .chips{
          display:flex;
          gap:8px;
          flex-wrap:wrap;
        }
        
        .chip{
          display:inline-flex;
          align-items:center;
          gap:8px;
          padding:8px 10px;
          border-radius:999px;
          background:rgba(255,255,255,.06);
          border:1px solid rgba(255,255,255,.10);
          font-size:12px;
          color:var(--muted);
        }
        
        .dot{width:8px;height:8px;border-radius:999px;background:var(--accent2)}
        .dot.ok{background:var(--ok)}
        .dot.warn{background:var(--warn)}
        
        /* =====================
           GRID + CARDS
        ===================== */
        .grid{
          display:grid;
          grid-template-columns:repeat(12,1fr);
          gap:12px;
          margin-top:14px;
        }
        
        .card{
          background: linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.05));
          border:1px solid rgba(255,255,255,.10);
          border-radius:var(--radius);
          box-shadow:var(--shadow);
          padding:14px;
        }
        
        .cardHeader{
          display:flex;
          align-items:center;
          justify-content:space-between;
          gap:12px;
          flex-wrap:wrap;
        }
        
        .cardHeader h2{
          margin:0;
          font-size:13px;
          color:var(--muted);
          font-weight:600;
        }
        
        /* =====================
           KPI
        ===================== */
        .kpiCard{grid-column:span 4}
        .big{font-size:34px;font-weight:900}
        .sub{font-size:12px;color:var(--muted2)}
        .mono{font-family:var(--mono)}
        
        .bar{
          height:10px;
          border-radius:999px;
          background:rgba(255,255,255,.08);
          overflow:hidden;
        }
        .barFill{
          height:100%;
          background:linear-gradient(90deg, var(--ok), var(--accent2));
        }
        
        /* =====================
           TABLES
        ===================== */
        .tableWrap{
          border:1px solid rgba(255,255,255,.10);
          border-radius:16px;
          overflow:hidden;
        }
        
        table{width:100%;border-collapse:separate;border-spacing:0}
        
        thead th{
          position:sticky;
          top:0;
          background:rgba(11,16,32,.85);
          font-size:12px;
          padding:10px 12px;
          text-align:left;
        }
        
        tbody td{
          padding:10px 12px;
          border-bottom:1px solid rgba(255,255,255,.08);
          font-size:13px;
        }
        
        .num{text-align:right;font-variant-numeric:tabular-nums}
        .empty{text-align:center;color:var(--muted);padding:14px}
        
        .scroll{max-height:420px;overflow:auto}
        
        /* =====================
           INPUTS + BUTTONS
        ===================== */
        .input{
          padding:10px 12px;
          border-radius:12px;
          border:1px solid rgba(255,255,255,.14);
          background:rgba(0,0,0,.18);
          color:var(--text);
          width:100%;
        }
        
        .btn{
          padding:10px 12px;
          border-radius:12px;
          border:1px solid rgba(255,255,255,.14);
          background:rgba(255,255,255,.08);
          color:var(--text);
          font-weight:700;
          cursor:pointer;
        }
        
        .btn.warn{background:rgba(245,158,11,.16)}
        .btn.danger{background:rgba(239,68,68,.16)}
        
        /* =====================
           ACTIONS (BUSCAR + JSON)
        ===================== */
        .actions{
          display:grid;
          grid-template-columns:minmax(220px,1fr) auto auto;
          gap:10px;
          align-items:center;
        }
        
        /* =====================
           QUICK ACTIONS (ADMIN)
        ===================== */
        .quickGrid{
          display:grid;
          grid-template-columns:repeat(12,1fr);
          gap:12px;
        }
        
        .qCard{
          grid-column:span 4;
          background:rgba(0,0,0,.16);
          border:1px solid rgba(255,255,255,.10);
          border-radius:16px;
          padding:12px;
        }
        
        .qCard h3{margin:0 0 8px;font-size:13px}
        
        .stack{display:flex;flex-direction:column;gap:8px}
        .row{display:flex;gap:8px;flex-wrap:wrap}
        
        /* =====================
           MODAL
        ===================== */
        .modalMask{
          position:fixed;inset:0;
          background:rgba(0,0,0,.55);
          display:none;
          align-items:center;
          justify-content:center;
          z-index:50;
        }
        
        .modal{
          width:min(920px,100%);
          border-radius:18px;
          background:rgba(0,0,0,.35);
          padding:14px;
        }
        
        /* =====================
           RESPONSIVE
        ===================== */
        @media (max-width:920px){
          .kpiCard{grid-column:span 6}
          .qCard{grid-column:span 12}
          .topbarInner{flex-direction:column;align-items:flex-start}
        }
        
        @media (max-width:560px){
          .kpiCard{grid-column:span 12}
          .actions{
            grid-template-columns:1fr 1fr;
          }
          .actions .input{
            grid-column:1 / -1;
          }
        }
      </style>
    </head>
    
    <body>
      <div class="topbar">
        <div class="topbarInner">
          <div class="brand">
            <div class="logo">CSF</div>
            <div class="title">
              <b>📊 CSF Docs · Admin</b>
              <span>Dashboard de uso y rendimiento</span>
            </div>
          </div>
          <div class="chips">
            <div class="chip" title="Ruta de stats">
              <span class="dot __DOTCLASS__"></span>
              <span><b>STATS</b> <span class="mono">__STATS_PATH__</span></span>
            </div>
            <div class="chip" title="Persistencia">
              <span class="dot __DOTCLASS__"></span>
              <span>__DISK_HINT__</span>
            </div>
            <div class="chip" title="Día más reciente detectado">
              <span class="dot"></span>
              <span>Último día: <span class="mono">__HOY_TOP__</span></span>
            </div>
          </div>
        </div>
      </div>
    
      <div class="wrap">
        <div class="grid">
    
          <div class="card kpiCard">
            <div class="cardHeader">
              <h2>Total solicitudes</h2>
              <span class="pill"><span class="dot"></span> Incluye fallos</span>
            </div>
            <div class="big">__TOTAL__</div>
            <div class="sub">Requests totales registrados en el sistema.</div>
          </div>
    
          <div class="card kpiCard">
            <div class="cardHeader">
              <h2>Total OK</h2>
              <span class="pill" style="background:rgba(34,197,94,.10);border-color:rgba(34,197,94,.28)">
                <span class="dot ok"></span> Constancias OK
              </span>
            </div>
            <div class="big">__OK__</div>
            <div class="sub">Constancias generadas correctamente.</div>
          </div>
    
          <div class="card kpiCard">
            <div class="cardHeader">
              <h2>Porcentaje OK</h2>
              <span class="pill" style="background:rgba(96,165,250,.10);border-color:rgba(96,165,250,.26)">
                <span class="dot"></span> Calidad
              </span>
            </div>
            <div class="kpi">
              <div class="big">__OK_RATE__%</div>
            </div>
            <div class="bar" style="margin-top:10px">
              <div class="barFill" style="width:__OK_RATE__%"></div>
            </div>
            <div class="sub">Porcentaje global de éxito (OK / total).</div>
          </div>
    
          <!-- =========================
               ✅ REEMPLAZO: Acciones rápidas (ordenado por secciones)
               ========================= -->
          <div class="card" style="grid-column: span 12;">
            <div class="cardHeader">
              <h2>Acciones rápidas</h2>
              <span class="sub">WhatsApp · Web · Permisos · Crítico</span>
            </div>
    
            <div class="quickGrid">
    
              <!-- WhatsApp -->
              <div class="qCard col4">
                <h3>📱 WhatsApp</h3>
                <div class="stack">
                  <div class="sub">WA ID (ej: 52xxxxxxxxxx)</div>
                  <input id="waId" class="input" placeholder="52899..." />
                  <input id="waReason" class="input" placeholder="Motivo (opcional)" />
                  <button class="btn danger" onclick="blockWA()">Bloquear WA</button>
                  <button class="btn" onclick="unblockWA()">Desbloquear WA</button>
                </div>
              </div>
    
              <!-- Permisos / Allowlist -->
              <div class="qCard col5">
                <h3>✅ Permisos / Allowlist</h3>
                <div class="stack">
                  <div class="sub">Acceso a generación (control por allowlist)</div>
                  <div class="row">
                    <button class="btn" onclick="allowAdd()">Permitir WA</button>
                    <button class="btn warn" onclick="allowRemove()">Quitar permiso</button>
                  </div>
                  <div class="row">
                    <button class="btn" onclick="allowToggle(true)">Activar allowlist</button>
                    <button class="btn danger" onclick="allowToggle(false)">Desactivar allowlist</button>
                  </div>
                  <div class="mutedSmall">Tip: usa “Permitir WA” para habilitar un número específico.</div>
                </div>
              </div>

              <!-- Pricing -->
            <div class="qCard col4">
              <h3>💲 Precios</h3>
              <div class="stack">
                <div class="sub">Usuario (WA o username)</div>
                <input id="pUser" class="input" placeholder="52899... o graciela.barajas" />
                <div class="sub">Tipo</div>
                <select id="pType" class="input">
                  <option value="RFC_IDCIF">RFC + IDCIF</option>
                  <option value="QR">QR (foto)</option>
                  <option value="CURP">CURP</option>
                </select>
                <div class="sub">Precio MXN</div>
                <input id="pPrice" class="input" placeholder="70" />
                <button class="btn" onclick="setUserPrice()">Guardar precio usuario</button>
                <button class="btn warn" onclick="delUserPrice()">Borrar precio usuario (tipo)</button>
                <button class="btn" onclick="openPricing()">Ver pricing JSON</button>
              </div>
            </div>
    
              <!-- Web -->
              <div class="qCard col3">
                <h3>🌐 Web</h3>
                <div class="stack">
                  <div class="sub">Usuario WEB (username)</div>
                  <input id="webUser" class="input" placeholder="graciela.barajas" />
                  <button class="btn" onclick="kickWeb()">Kick sesión (WEB)</button>
                  <div style="height:6px"></div>
                  <div class="sub">Consultas</div>
                  <button class="btn" onclick="openUser()">Abrir stats usuario</button>
                </div>
              </div>
    
              <!-- Datos / Billing -->
              <div class="qCard col6">
                <h3>💳 Datos / Billing</h3>
                <div class="stack">
                  <div class="sub">RFC a borrar (deduplicación + facturación)</div>
                  <input id="rfcDel" class="input" placeholder="VAEC9409082X6" />
                  <button class="btn warn" onclick="deleteRFC()">Borrar RFC</button>
    
                  <div style="height:6px"></div>
                  <div class="sub">Consultas de facturación</div>
                  <div class="row">
                    <button class="btn" onclick="openBilling()">Ver facturación global</button>
                    <button class="btn" onclick="openBillingUser()">Ver facturación por usuario</button>
                  </div>
                </div>
              </div>
    
              <!-- Zona crítica -->
              <div class="qCard col6 dangerZone">
                <h3>⚠️ Zona crítica</h3>
                <div class="stack">
                  <div class="sub">Acciones irreversibles</div>
                  <button class="btn danger" onclick="resetAll()">Reset TODO (WA + WEB)</button>
                  <div class="mutedSmall">Esto borra histórico. Úsalo solo si estás seguro.</div>
                </div>
              </div>
    
            </div>
    
            <pre id="actionOut" class="mono" style="margin-top:12px;white-space:pre-wrap;background:rgba(0,0,0,.18);border:1px solid rgba(255,255,255,.10);border-radius:14px;padding:12px;max-height:260px;overflow:auto">Listo.</pre>
          </div>
    
          <!-- ====== ADDON: Billing + Stats visual ====== -->
          <div class="card" style="grid-column: span 12;">
            <div class="cardHeader">
              <h2>💳 Billing & Stats (visual)</h2>
              <div class="actions">
                <input id="qUser" class="input" placeholder="Buscar usuario (WA o username)..." oninput="renderBillingTables()" />
                <button class="btn" onclick="openJson('billing')">Billing JSON</button>
                <button class="btn" onclick="openJson('stats')">Stats JSON</button>
              </div>
            </div>
    
            <div class="grid" style="margin-top:10px">
              <div class="card" style="grid-column: span 4; box-shadow:none;">
                <div class="cardHeader"><h2>Global</h2></div>
                <div class="big" id="bRevenue">—</div>
                <div class="sub" id="bMeta">—</div>
                <div class="miniBar" style="margin-top:10px"><div class="miniFill" id="bFill"></div></div>
                <div class="mutedSmall" style="margin-top:8px" id="bHint">—</div>
              </div>
    
              <div class="card" style="grid-column: span 8; box-shadow:none;">
                <div class="cardHeader"><h2>Por usuario (billing)</h2></div>
                <div class="tableWrap">
                  <div class="scroll">
                    <table>
                      <thead>
                        <tr>
                          <th>Usuario</th>
                          <th class="num" style="width:110px">Facturado</th>
                          <th class="num" style="width:140px">Ganancia</th>
                          <th style="width:220px">Progreso</th>
                          <th style="width:120px">Acción</th>
                        </tr>
                      </thead>
                      <tbody id="tblBillingUsers">
                        <tr><td colspan="5" class="empty">Cargando…</td></tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
    
            <div class="card" style="margin-top:12px; box-shadow:none;">
              <div class="cardHeader"><h2>Por usuario (stats)</h2></div>
              <div class="tableWrap">
                <div class="scroll">
                  <table>
                    <thead>
                      <tr>
                        <th>Usuario</th>
                        <th class="num" style="width:120px">Solicitudes</th>
                        <th class="num" style="width:90px">OK</th>
                        <th style="width:220px">Tasa</th>
                        <th style="width:120px">Acción</th>
                      </tr>
                    </thead>
                    <tbody id="tblStatsUsers">
                      <tr><td colspan="5" class="empty">Cargando…</td></tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
    
          <div class="card wide">
            <div class="cardHeader">
              <h2>Últimos 14 días</h2>
              <span class="sub">Solicitudes · OK · tasa</span>
            </div>
            <div class="tableWrap">
              <div class="scroll">
                <table>
                  <thead>
                    <tr>
                      <th style="width:140px">Día</th>
                      <th class="num" style="width:110px">Solicitudes</th>
                      <th class="num" style="width:90px">OK</th>
                      <th style="width:160px">Tasa OK</th>
                    </tr>
                  </thead>
                  <tbody>
                    __HTML_DAYS__
                  </tbody>
                </table>
              </div>
            </div>
          </div>
    
          <div class="card side">
            <div class="cardHeader">
              <h2>Últimos RFC OK</h2>
              <span class="sub">Últimos 30</span>
            </div>
            <div class="chipsBox">
              __HTML_RFCS__
            </div>
            <div class="sub" style="margin-top:10px">
              Tip: aquí puedes detectar duplicados o abuso rápido.
            </div>
          </div>
    
          <div class="card" style="grid-column: span 12;">
            <div class="cardHeader">
              <h2>Uso por usuario (hoy)</h2>
              <span class="sub">Ordenado por día y consumo</span>
            </div>
    
            <div class="tableWrap">
              <div class="scroll">
                <table>
                  <thead>
                    <tr>
                      <th>Usuario</th>
                      <th class="num" style="width:110px">Contado</th>
                      <th class="num" style="width:90px">OK</th>
                      <th style="width:160px">Tasa OK</th>
                    </tr>
                  </thead>
                  <tbody>
                    __HTML_USERS__
                  </tbody>
                </table>
              </div>
            </div>
          </div>
    
        </div>
    
      </div>
    
      <!-- ====== ADDON: Modal ====== -->
      <div class="modalMask" id="mask" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
          <div class="modalHead">
            <div>
              <b id="mTitle">Detalle</b>
              <div class="mutedSmall" id="mSub">—</div>
            </div>
            <button class="btn" onclick="closeModal()">Cerrar</button>
          </div>
          <div class="modalBody">
            <pre id="mPre">{}</pre>
          </div>
        </div>
      </div>
    
      <script>
          const ADMIN_TOKEN = "__ADMIN_TOKEN__";
    
          function out(x){
            const el = document.getElementById("actionOut");
            el.textContent = typeof x === "string" ? x : JSON.stringify(x, null, 2);
          }
    
          async function api(path, method="GET", body=null){
            const headers = {};
            if (ADMIN_TOKEN) headers["X-Admin-Token"] = ADMIN_TOKEN;
            if (body) headers["Content-Type"] = "application/json";
    
            const res = await fetch(path, { method, headers, body: body ? JSON.stringify(body) : null });
            const txt = await res.text();
            let data;
            try { data = JSON.parse(txt); } catch { data = { raw: txt }; }
            if (!res.ok) throw { status: res.status, data };
            return data;
          }
    
          function waId(){ return (document.getElementById("waId").value || "").trim(); }
          function waReason(){ return (document.getElementById("waReason").value || "").trim(); }
          function rfcDel(){ return (document.getElementById("rfcDel").value || "").trim().toUpperCase(); }
          function webUser(){ return (document.getElementById("webUser").value || "").trim(); }
    
          async function blockWA(){
            try{
              const id = waId();
              if(!id) return out("Falta WA ID");
              const data = await api("/admin/wa/block", "POST", { wa_id: id, reason: waReason() });
              out(data);
            }catch(e){ out(e); }
          }
    
          async function unblockWA(){
            try{
              const id = waId();
              if(!id) return out("Falta WA ID");
              const data = await api("/admin/wa/unblock", "POST", { wa_id: id });
              out(data);
            }catch(e){ out(e); }
          }
    
          async function deleteRFC(){
            try{
              const r = rfcDel();
              if(!r) return out("Falta RFC");
              const data = await api("/admin/rfc/delete", "POST", { rfc: r });
              out(data);
            }catch(e){ out(e); }
          }
    
          async function kickWeb(){
            try{
              const u = webUser();
              if(!u) return out("Falta username");
              const data = await api("/admin/kick", "POST", { username: u });
              out(data);
            }catch(e){ out(e); }
          }
    
          function openUser(){
            const u = webUser();
            if(!u) return out("Falta username");
            const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
            window.open("/admin/user/" + encodeURIComponent(u) + q, "_blank");
          }
    
          function openBilling(){
            const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
            window.open("/admin/billing" + q, "_blank");
          }
    
          function openBillingUser(){
            const u = waId() || webUser();
            if(!u) return out("Pon WA ID o username");
            const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
            window.open("/admin/billing/user/" + encodeURIComponent(u) + q, "_blank");
          }
    
          // ====== ADDON: Billing + Stats visual ======
          let CACHE = { billing: null, stats: null };
    
          function money(n){
            n = Number(n || 0);
            return n.toLocaleString('es-MX', { style:'currency', currency:'MXN' });
          }
          function pct(a,b){
            a = Number(a || 0); b = Number(b || 0);
            return b > 0 ? (a/b*100) : 0;
          }
    
          async function reloadBilling(){
            try{
              // usa tu mismo token (ADMIN_TOKEN) pero por querystring para tus endpoints GET
              const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
    
              // endpoints existentes
              CACHE.billing = await fetch("/admin/billing" + q, { cache:"no-store" }).then(r=>r.json());
              CACHE.stats   = await fetch("/stats" + q, { cache:"no-store" }).then(r=>r.json()).catch(()=> ({}));
    
              renderBillingGlobal();
              renderBillingTables();
              out({ ok:true, msg:"Billing/Stats actualizado" });
            }catch(e){
              console.error(e);
              out({ ok:false, error:e });
            }
          }
    
          function renderBillingGlobal(){
            const b = CACHE.billing || {};
            const price = Number(b.price_mxn || 0);
            const billed = Number(b.total_billed || 0);
            const rev = Number(b.total_revenue_mxn || 0);
    
            const elRev = document.getElementById("bRevenue");
            const elMeta = document.getElementById("bMeta");
            const elHint = document.getElementById("bHint");
            const elFill = document.getElementById("bFill");
    
            if(!elRev) return;
    
            elRev.textContent = money(rev);
            elMeta.textContent = `Facturado: ${billed.toLocaleString()} · Precio: ${money(price)}`;
            elFill.style.width = Math.min(100, billed * 5) + "%";
            elHint.textContent = price > 0 ? "Precio activo y revenue calculándose." : "⚠️ PRICE_PER_OK_MXN está en 0 (revenue siempre será 0).";
          }
    
          function renderBillingTables(){
            const q = (document.getElementById("qUser")?.value || "").trim().toLowerCase();
    
            // --- billing by user ---
            const byUser = (CACHE.billing && CACHE.billing.by_user) ? CACHE.billing.by_user : {};
            let rowsB = Object.entries(byUser).map(([user, info]) => {
              info = info || {};
              return {
                user,
                billed: Number(info.billed || 0),
                rev: Number(info.revenue_mxn || 0),
                last: info.last || "",
                rfcs: (info.rfcs || []).slice(-3).reverse()
              };
            });
    
            rowsB.sort((a,b)=> (b.rev - a.rev) || (b.billed - a.billed));
            if(q) rowsB = rowsB.filter(x => x.user.toLowerCase().includes(q));
    
            const maxBilled = Math.max(1, ...rowsB.map(x=>x.billed));
            const tb = document.getElementById("tblBillingUsers");
            if(tb){
              tb.innerHTML = rowsB.length ? rowsB.map(x=>{
                const w = Math.round((x.billed / maxBilled) * 100);
                const chips = x.rfcs.length
                  ? x.rfcs.map(r=>`<span class="chip mono" style="padding:6px 8px">${r}</span>`).join("")
                  : `<span class="mutedSmall">—</span>`;
                return `
                  <tr>
                    <td>
                      <div style="font-weight:900">${x.user}</div>
                      <div class="mutedSmall mono">${x.last || ""}</div>
                      <div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">${chips}</div>
                    </td>
                    <td class="num">${x.billed.toLocaleString()}</td>
                    <td class="num">${money(x.rev)}</td>
                    <td>
                      <div class="miniBar"><div class="miniFill" style="width:${w}%"></div></div>
                      <div class="mutedSmall" style="margin-top:6px">${w}% del top</div>
                    </td>
                    <td><button class="btn" onclick="openUserDetail('${encodeURIComponent(x.user)}')">Detalle</button></td>
                  </tr>
                `;
              }).join("") : `<tr><td colspan="5" class="empty">Sin usuarios (o filtro).</td></tr>`;
            }
    
            // --- stats por usuario (desde /stats) ---
            const pu = (CACHE.stats && CACHE.stats.por_usuario) ? CACHE.stats.por_usuario : {};
            let rowsS = Object.entries(pu).map(([user, info]) => {
              info = info || {};
              const req = Number(info.count || 0);
              const ok = Number(info.success || 0);
              return { user, req, ok, rate: pct(ok, req), hoy: info.hoy || "" };
            });
    
            rowsS.sort((a,b)=> (String(b.hoy).localeCompare(String(a.hoy))) || (b.req - a.req) || (b.ok - a.ok));
            if(q) rowsS = rowsS.filter(x => x.user.toLowerCase().includes(q));
    
            const maxReq = Math.max(1, ...rowsS.map(x=>x.req));
            const ts = document.getElementById("tblStatsUsers");
            if(ts){
              ts.innerHTML = rowsS.length ? rowsS.map(x=>{
                const w = Math.round((x.req / maxReq) * 100);
                return `
                  <tr>
                    <td>
                      <div style="font-weight:900">${x.user}</div>
                      <div class="mutedSmall mono">${x.hoy || ""}</div>
                    </td>
                    <td class="num">${x.req.toLocaleString()}</td>
                    <td class="num">${x.ok.toLocaleString()}</td>
                    <td>
                      <div class="miniBar"><div class="miniFill" style="width:${Math.round(x.rate)}%"></div></div>
                      <div class="mutedSmall" style="margin-top:6px">${x.rate.toFixed(1)}%</div>
                    </td>
                    <td><button class="btn" onclick="openUserDetail('${encodeURIComponent(x.user)}')">Detalle</button></td>
                  </tr>
                `;
              }).join("") : `<tr><td colspan="5" class="empty">Sin stats (o filtro).</td></tr>`;
            }
          }
    
          async function openUserDetail(userEnc){
            try{
              const user = decodeURIComponent(userEnc);
              const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
    
              const billingUser = await fetch("/admin/billing/user/" + encodeURIComponent(user) + q, { cache:"no-store" }).then(r=>r.json());
              let okrfcs = null;
              try{
                okrfcs = await fetch("/admin/okrfcs/" + encodeURIComponent(user) + q, { cache:"no-store" }).then(r=>r.json());
              }catch(_){ okrfcs = { ok:false, note:"/admin/okrfcs no disponible" }; }
    
              const statsUser = (CACHE.stats && CACHE.stats.por_usuario) ? (CACHE.stats.por_usuario[user] || null) : null;
    
              openModal("👤 " + user, "Detalle combinado (billing + stats + okrfcs)", JSON.stringify({ billingUser, statsUser, okrfcs }, null, 2));
            }catch(e){
              out(e);
            }
          }
    
          function openJson(which){
            if(which === "billing") return openModal("Billing global", "Fuente: /admin/billing", JSON.stringify(CACHE.billing || {}, null, 2));
            if(which === "stats") return openModal("Stats", "Fuente: /stats", JSON.stringify(CACHE.stats || {}, null, 2));
          }
    
          function openModal(title, sub, pre){
            document.getElementById("mTitle").textContent = title;
            document.getElementById("mSub").textContent = sub;
            document.getElementById("mPre").textContent = pre || "{}";
            document.getElementById("mask").style.display = "flex";
          }
          function closeModal(){
            document.getElementById("mask").style.display = "none";
          }
    
          async function resetAll(){
            if(!confirm("¿Seguro? Esto borra TODO el histórico (WA + WEB).")) return;
            try{
              const data = await api("/admin/reset_all", "POST", {});
              out(data);
              await reloadBilling();
            }catch(e){ out(e); }
          }
    
          async function allowAdd(){
            try{
              const id = waId();
              if(!id) return out("Falta WA ID");
              const data = await api("/admin/wa/allow/add", "POST", { wa_id: id, note: waReason() });
              out(data);
            }catch(e){ out(e); }
          }
    
          async function allowRemove(){
            try{
              const id = waId();
              if(!id) return out("Falta WA ID");
              const data = await api("/admin/wa/allow/remove", "POST", { wa_id: id });
              out(data);
            }catch(e){ out(e); }
          }
    
          async function allowToggle(enabled){
            try{
              const data = await api("/admin/wa/allow/enabled", "POST", { enabled: !!enabled });
              out(data);
            }catch(e){ out(e); }
          }

        function pUser(){ return (document.getElementById("pUser").value || "").trim(); }
        function pType(){ return (document.getElementById("pType").value || "RFC_IDCIF").trim(); }
        function pPrice(){ return parseInt((document.getElementById("pPrice").value || "0").trim(), 10) || 0; }
        
        async function setUserPrice(){
          try{
            const u = pUser();
            if(!u) return out("Falta usuario en precios");
            const data = await api("/admin/pricing/user/set", "POST", { user:u, type:pType(), price_mxn:pPrice() });
            out(data);
            await reloadBilling();
          }catch(e){ out(e); }
        }
        async function delUserPrice(){
          try{
            const u = pUser();
            if(!u) return out("Falta usuario en precios");
            const data = await api("/admin/pricing/user/delete", "POST", { user:u, type:pType() });
            out(data);
            await reloadBilling();
          }catch(e){ out(e); }
        }
        function openPricing(){
          const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
          window.open("/admin/pricing" + q, "_blank");
        }
    
          // auto-carga al abrir /admin
          reloadBilling();
      </script>
    </body>
    </html>
    """
    
    # Reemplazos seguros
    html = (html
        .replace("__ADMIN_TOKEN__", ADMIN_STATS_TOKEN or "")
        .replace("__STATS_PATH__", STATS_PATH or "")
        .replace("__DISK_HINT__", disk_hint or "")
        .replace("__HOY_TOP__", hoy_top or "—")
        .replace("__DOTCLASS__", dot_class)
        .replace("__TOTAL__", str(total))
        .replace("__OK__", str(ok))
        .replace("__OK_RATE__", f"{ok_rate:.1f}")
        .replace("__HTML_DAYS__", html_days)
        .replace("__HTML_USERS__", html_users)
        .replace("__HTML_RFCS__", html_rfcs)
    )
    
    return html

if __name__ == "__main__":
    app.run(debug=True, port=5000)








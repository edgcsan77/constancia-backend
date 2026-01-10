# rfc.py
# -*- coding: utf-8 -*-
import os
import sys
import re
import ssl
import tempfile
import json
import jwt
import hashlib
import random
import base64
import time
import traceback

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

CURP_RE = re.compile(r"^[A-Z][AEIOUX][A-Z]{2}\d{2}(0\d|1[0-2])(0\d|[12]\d|3[01])[HM][A-Z]{5}[0-9A-Z]\d$", re.I)
RFC_RE  = re.compile(r"^([A-ZÑ&]{3,4})\d{6}([A-Z0-9]{3})$", re.I)

ADMIN_KEY = os.getenv("ADMIN_KEY", "")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = "edgcsan77"
GITHUB_REPO = "validacion-sat"
GITHUB_BRANCH = "main"
PERSONAS_PATH = "public/data/personas.json"

CACHE_LOCK = threading.Lock()

# Archivo del cache (relativo al proyecto /app en Render normalmente)
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
CACHE_FILE = os.path.join(CACHE_DIR, "cache_checkid.json")

# TTL default: 7 días (configurable con env)
CACHE_TTL_SECONDS = int(os.getenv("CHECKID_CACHE_TTL", str(7 * 24 * 3600)))

def _cache_load() -> dict:
    try:
        if not os.path.exists(CACHE_FILE):
            return {}

        # si el archivo está vacío, trátalo como cache vacío
        if os.path.getsize(CACHE_FILE) == 0:
            return {}

        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            raw = f.read().strip()

        if not raw:
            return {}

        data = json.loads(raw)
        return data if isinstance(data, dict) else {}

    except json.JSONDecodeError as e:
        print("CACHE LOAD ERROR:", repr(e))
        # respaldo y reinicio limpio
        try:
            bad = CACHE_FILE + f".bad.{int(time.time())}"
            os.replace(CACHE_FILE, bad)
            print("CACHE CORRUPT -> moved to", bad)
        except Exception as e2:
            print("CACHE BACKUP ERROR:", repr(e2))
        return {}

    except Exception as e:
        print("CACHE LOAD ERROR:", repr(e))
        return {}

def _cache_save(data: dict) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)

    tmp = CACHE_FILE + ".tmp"
    try:
        # limpia tmp viejo si existe
        if os.path.exists(tmp):
            try: os.remove(tmp)
            except Exception: pass

        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CACHE_FILE)
    except Exception as e:
        print("CACHE SAVE ERROR:", repr(e))
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def cache_get(key: str):
    if not key:
        return None
    now = int(time.time())

    with CACHE_LOCK:
        db = _cache_load()
        row = db.get(key)
        if not isinstance(row, dict):
            return None

        exp = row.get("exp")
        val = row.get("val")

        # Si no tiene exp, trátalo como inválido
        if not isinstance(exp, int):
            db.pop(key, None)
            _cache_save(db)
            return None

        if exp <= now:
            # expirado
            db.pop(key, None)
            _cache_save(db)
            return None

        return val

def cache_set(key: str, value, ttl_seconds: int | None = None):
    if not key:
        return
    now = int(time.time())
    ttl = int(ttl_seconds or CACHE_TTL_SECONDS)
    exp = now + max(60, ttl)  # mínimo 60s para evitar exp inmediatas

    with CACHE_LOCK:
        db = _cache_load()
        db[key] = {"exp": exp, "val": value}
        _cache_save(db)

def cache_del(key: str):
    if not key:
        return
    with CACHE_LOCK:
        db = _cache_load()
        if key in db:
            db.pop(key, None)
            _cache_save(db)

def github_update_personas(d3_key: str, persona: dict):
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    file_url = (
        f"https://api.github.com/repos/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/contents/{PERSONAS_PATH}"
        f"?ref={GITHUB_BRANCH}"
    )

    r = requests.get(file_url, headers=headers)
    if r.status_code == 404:
        current = {}
        sha = None
    elif r.status_code == 200:
        data = r.json()
        sha = data["sha"]
        current = json.loads(
            base64.b64decode(data["content"]).decode("utf-8")
        )
    else:
        raise Exception(f"Error leyendo personas.json: {r.text}")

    current[d3_key] = persona

    new_content = base64.b64encode(
        json.dumps(current, indent=2, ensure_ascii=False).encode("utf-8")
    ).decode("utf-8")

    payload = {
        "message": f"update personas.json: {d3_key}",
        "content": new_content,
        "branch": GITHUB_BRANCH
    }

    if sha:
        payload["sha"] = sha

    r2 = requests.put(file_url, headers=headers, json=payload)
    if r2.status_code not in (200, 201):
        raise Exception(f"Error commiteando personas.json: {r2.text}")

    return True

def require_admin():
    sent = request.headers.get("X-Admin-Key", "")
    if not ADMIN_KEY or sent != ADMIN_KEY:
        return False
    return True

def normalize_text(s: str) -> str:
    return (s or "").strip()

def looks_like_qr_payload(s: str) -> bool:
    s = (s or "").lower()
    # QR del validador / parámetros D1 D2 D3
    if "validadorqr" in s or "faces/pages/mobile/validadorqr.jsf" in s:
        return True
    if "d1=" in s and "d2=" in s and "d3=" in s:
        return True
    return False

def looks_like_idcif(s: str) -> bool:
    # ajusta si tu IDCIF tiene un formato específico; esto es un heurístico seguro
    s = (s or "").upper()
    return ("IDCIF" in s) or ("CIF" in s and len(s) >= 8)

def classify_input_for_personas(raw_text: str):
    """
    Regresa:
      - ("only_curp", curp) si es SOLO CURP
      - ("only_rfc", rfc)   si es SOLO RFC
      - (None, None)        si NO aplica (qr, idcif, o múltiples datos)
    """
    t = normalize_text(raw_text)

    if not t:
        return (None, None)

    # Si parece QR / link QR => NO
    if looks_like_qr_payload(t):
        return (None, None)

    # Si trae IDCIF explícito => NO
    if looks_like_idcif(t):
        return (None, None)

    # Si el texto tiene espacios o separadores típicos de combos => NO (evita CURP+RFC)
    # ejemplo: "RFC:XXXX CURP:YYYY" o "XXXX|YYYY"
    if any(sep in t for sep in [" ", "\n", "\t", "|", ",", ";"]):
        # PERO si aun así es exactamente un CURP o exactamente un RFC, sí dejamos pasar
        # (ej: el usuario pegó con espacios al inicio/fin). Para eso validamos "token único":
        tokens = [x for x in re.split(r"[\s\|\.,;]+", t) if x]
        if len(tokens) != 1:
            return (None, None)
        t = tokens[0]

    # SOLO CURP
    if CURP_RE.match(t):
        return ("only_curp", t.upper())

    # SOLO RFC
    if RFC_RE.match(t):
        return ("only_rfc", t.upper())

    return (None, None)

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
# Render Disk: monta /data (Persistent Disk) y guarda ahí.
STATS_PATH = os.getenv("STATS_PATH", "/data/stats.json")
ADMIN_STATS_TOKEN = os.getenv("ADMIN_STATS_TOKEN", "")

# Asegurar carpeta para evitar que escriba en otro lado o falle silencioso
_stats_dir = os.path.dirname(STATS_PATH)
if _stats_dir and not os.path.exists(_stats_dir):
    os.makedirs(_stats_dir, exist_ok=True)

print("STATS_PATH ->", STATS_PATH, "exists?", os.path.exists(STATS_PATH))

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

_MESES = {
    "ENERO": "01", "FEBRERO": "02", "MARZO": "03", "ABRIL": "04",
    "MAYO": "05", "JUNIO": "06", "JULIO": "07", "AGOSTO": "08",
    "SEPTIEMBRE": "09", "SETIEMBRE": "09", "OCTUBRE": "10",
    "NOVIEMBRE": "11", "DICIEMBRE": "12",
}

def _z2(n) -> str:
    try:
        return f"{int(n):02d}"
    except Exception:
        return ""

def _to_dd_mm_aaaa_dash(value: str) -> str:
    """
    Convierte a dd-mm-aaaa desde:
      - ISO: 2007-12-03 / 2007-12-03T00:00:00
      - dd/mm/aaaa
      - dd-mm-aaaa (lo deja)
      - '03 DE DICIEMBRE DE 2007'
      - '03-12-2007' (lo deja)
    Si no puede, regresa "".
    """
    if not value:
        return ""

    s = str(value).strip()
    if not s:
        return ""

    # ISO yyyy-mm-dd...
    m = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mm, d = m.group(1), m.group(2), m.group(3)
        return f"{d}-{mm}-{y}"

    # dd/mm/aaaa
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$", s)
    if m:
        d, mm, y = _z2(m.group(1)), _z2(m.group(2)), m.group(3)
        return f"{d}-{mm}-{y}"

    # dd-mm-aaaa
    m = re.match(r"^\s*(\d{1,2})-(\d{1,2})-(\d{4})\s*$", s)
    if m:
        d, mm, y = _z2(m.group(1)), _z2(m.group(2)), m.group(3)
        return f"{d}-{mm}-{y}"

    # 'DD DE MES DE AAAA'
    up = s.upper()
    m = re.match(r"^\s*(\d{1,2})\s+DE\s+([A-ZÁÉÍÓÚÜÑ]+)\s+DE\s+(\d{4})\s*$", up)
    if m:
        d = _z2(m.group(1))
        mes_txt = m.group(2).replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U").replace("Ü","U")
        mm = _MESES.get(mes_txt, "")
        y = m.group(3)
        if mm:
            return f"{d}-{mm}-{y}"

    return ""

def _al_from_entidad(entidad: str) -> str:
    e = (entidad or "").strip().upper()
    if not e:
        return "CIUDAD DE MÉXICO 1"  # fallback seguro
    return f"{e} 1"

def _parse_birth_year(fecha_nac: str) -> int | None:
    """
    Acepta:
      - '1978-09-20T00:00:00'
      - '1978-09-20'
      - '20/09/1978' (si algún día llega así)
    """
    if not fecha_nac:
        return None
    s = str(fecha_nac).strip()
    try:
        # ISO: YYYY-MM-DD...
        if re.match(r"^\d{4}-\d{2}-\d{2}", s):
            return int(s[:4])
        # DD/MM/YYYY
        if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
            return int(s.split("/")[-1])
    except Exception:
        return None
    return None

def _det_rng(seed_text: str) -> random.Random:
    """
    Random determinístico para que NO cambie entre reintentos.
    """
    h = hashlib.sha256((seed_text or "").encode("utf-8")).hexdigest()
    seed_int = int(h[:16], 16)  # 64 bits
    return random.Random(seed_int)

def _fake_date_dd_de_mmmm_de_aaaa(year: int, seed_key: str, salt: str) -> str:
    """
    Día 1-30, mes 1-12, año fijo.
    """
    rng = _det_rng(f"{seed_key}|{salt}")
    day = rng.randint(1, 30)
    month = rng.randint(1, 12)
    mes = MESES_ES.get(month, str(month))
    return f"{day:02d} DE {mes} DE {year}"

def _fake_date_dd_mm_yyyy(year: int, seed_key: str, salt: str) -> str:
    """
    Para FECHA_ALTA si la quieres en formato  dd/mm/yyyy.
    """
    rng = _det_rng(f"{seed_key}|{salt}|alta")
    day = rng.randint(1, 30)
    month = rng.randint(1, 12)
    return f"{day:02d}/{month:02d}/{year}"

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

def _fecha_lugar_mun_ent(municipio: str, entidad: str) -> str:
    hoy = hoy_mexico()
    fecha = f"{hoy.day:02d} DE {MESES_ES[hoy.month]} DE {hoy.year}"

    mun = (municipio or "").strip().upper()
    ent = (entidad or "").strip().upper()

    if mun and ent:
        return f"{mun}, {ent} A {fecha}"
    if mun:
        return f"{mun} A {fecha}"
    if ent:
        return f"{ent} A {fecha}"
    return fecha
    
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

# ================== NUEVO: PUBLICAR DATOS EN VALIDACION-SAT ==================

VALIDACION_SAT_BASE = (os.getenv("VALIDACION_SAT_BASE", "") or "").rstrip("/")
VALIDACION_SAT_APIKEY = (os.getenv("VALIDACION_SAT_APIKEY", "") or "").strip()
VALIDACION_SAT_TIMEOUT = int(os.getenv("VALIDACION_SAT_TIMEOUT", "8") or "8")

def validacion_sat_enabled() -> bool:
    return bool(VALIDACION_SAT_BASE and VALIDACION_SAT_APIKEY)

def validacion_sat_publish(datos: dict, input_type: str) -> str | None:
    """
    Publica los datos generados (CURP/RFC_ONLY) en validacion-sat para que el QR sea funcional.
    Regresa la URL pública que se debe meter al QR.
    
    Requiere que validacion-sat exponga:
      POST {BASE}/api/validaciones
      -> { ok:true, token:"abc123", url:"https://.../v/abc123" }
    """
    if not validacion_sat_enabled():
        return None

    rfc = (datos.get("RFC") or "").strip().upper()
    curp = (datos.get("CURP") or "").strip().upper()

    # Idempotencia: si reintentan, debe regresar el mismo token idealmente
    # (en validacion-sat puedes usar este idempotency_key para upsert)
    idem = f"{input_type}:{curp or rfc}"

    payload = {
        "idempotency_key": idem,
        "source": "constancia-backend",
        "input_type": input_type,
        "issued_at": datos.get("FECHA_CORTA") or "",
        "data": {
            "RFC": rfc,
            "CURP": curp,
            "NOMBRE": datos.get("NOMBRE") or "",
            "PRIMER_APELLIDO": datos.get("PRIMER_APELLIDO") or "",
            "SEGUNDO_APELLIDO": datos.get("SEGUNDO_APELLIDO") or "",
            "NOMBRE_ETIQUETA": datos.get("NOMBRE_ETIQUETA") or "",
            "CP": datos.get("CP") or "",
            "COLONIA": datos.get("COLONIA") or "",
            "LOCALIDAD": datos.get("LOCALIDAD") or "",
            "ENTIDAD": datos.get("ENTIDAD") or "",
            "REGIMEN": datos.get("REGIMEN") or "",
            "FECHA_ALTA": datos.get("FECHA_ALTA") or "",
            "FECHA_INICIO": datos.get("FECHA_INICIO") or "",
            "FECHA_ULTIMO": datos.get("FECHA_ULTIMO") or "",
            "ESTATUS": datos.get("ESTATUS") or "",
            "IDCIF_ETIQUETA": datos.get("IDCIF_ETIQUETA") or "",
            "FECHA_NACIMIENTO": datos.get("FECHA_NACIMIENTO") or "",
        }
    }

    url = f"{VALIDACION_SAT_BASE}/api/validaciones"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Api-Key": VALIDACION_SAT_APIKEY,
        "User-Agent": "CSFDocs/1.0",
    }

    r = requests.post(url, json=payload, headers=headers, timeout=VALIDACION_SAT_TIMEOUT)
    if not r.ok:
        print("VALIDACION_SAT publish ERROR:", r.status_code, r.text[:2000])
        return None

    j = r.json() or {}
    # preferimos url directa si viene
    public_url = (j.get("url") or "").strip()
    token = (j.get("token") or "").strip()

    if public_url:
        return public_url
    if token:
        return f"{VALIDACION_SAT_BASE}/v/{urllib.parse.quote(token)}"
    return None

def elegir_url_qr(datos: dict, input_type: str, rfc_val: str, idcif_val: str) -> str:
    input_type = (input_type or "").upper().strip()
    rfc_val = (rfc_val or "").strip().upper()
    idcif_val = (idcif_val or "").strip()

    # ✅ 0) Para SOLO CURP / SOLO RFC: SIEMPRE usar validadorqr.jsf con D3 = IDCIF_RFC
    if input_type in ("CURP", "RFC_ONLY") and VALIDACION_SAT_BASE and idcif_val and rfc_val:
        d3 = f"{idcif_val}_{rfc_val}"
        return (
            f"{VALIDACION_SAT_BASE}/app/qr/faces/pages/mobile/validadorqr.jsf"
            f"?D1=10&D2=1&D3={urllib.parse.quote(d3)}"
        )

    # 1) Si ya se publicó en validacion-sat (otros casos), úsalo
    qr_url_pub = (datos.get("QR_URL") or "").strip()
    if qr_url_pub:
        return qr_url_pub

    # 2) QR oficial SAT SOLO cuando sea RFC_IDCIF y haya idCIF real
    if input_type == "RFC_IDCIF" and idcif_val:
        d3 = f"{idcif_val}_{rfc_val}"
        return (
            "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
            f"?D1=10&D2=1&D3={d3}"
        )

    # 3) Fallback seguro
    if VALIDACION_SAT_BASE:
        return f"{VALIDACION_SAT_BASE}/v?rfc={urllib.parse.quote_plus(rfc_val)}"

    return "https://siat.sat.validacion-sat.org"

def reemplazar_en_documento(ruta_entrada, ruta_salida, datos, input_type):
    rfc_val = (datos.get("RFC_ETIQUETA") or datos.get("RFC", "")).strip()
    idcif_val = (datos.get("IDCIF_ETIQUETA") or "").strip()

    # ✅ aquí se decide el QR (UNA sola línea)
    url_qr = elegir_url_qr(datos, input_type, rfc_val, idcif_val)

    # ✅ generar una sola vez
    qr_bytes, barcode_bytes = generar_qr_y_barcode(url_qr, rfc_val)

    # hard rules por si llegan diferentes:
    if datos.get("COLONIA"):
        datos["COLONIA"] = str(datos["COLONIA"]).upper()

    if input_type in ("CURP","RFC_ONLY"):
        datos["TIPO_VIALIDAD"] = "CALLE"
        datos["VIALIDAD"] = "SIN NOMBRE"
        datos["NO_INTERIOR"] = ""

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
        "{{ FECHA INICIO }}": datos.get("FECHA_INICIO_DOC", ""),
        "{{ ESTATUS }}": datos.get("ESTATUS", ""),
        "{{ FECHA ULTIMO }}": datos.get("FECHA_ULTIMO_DOC", ""),
        "{{ CP }}": datos.get("CP", ""),
        "{{ TIPO VIALIDAD }}": datos.get("TIPO_VIALIDAD", ""),
        "{{ VIALIDAD }}": datos.get("VIALIDAD", ""),
        "{{ NO EXTERIOR }}": datos.get("NO_EXTERIOR", ""),
        "{{ NO INTERIOR }}": datos.get("NO_INTERIOR", ""),
        "{{ COLONIA }}": datos.get("COLONIA", ""),
        "{{ LOCALIDAD }}": datos.get("LOCALIDAD", ""),
        "{{ ENTIDAD }}": datos.get("ENTIDAD", ""),
        "{{ REGIMEN }}": datos.get("REGIMEN", ""),
        "{{ FECHA ALTA }}": datos.get("FECHA_ALTA_DOC", ""),
        "{{ FECHA NACIMIENTO }}": datos.get("FECHA_NACIMIENTO", ""),
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
ALLOWED_ORIGINS = (os.getenv("CORS_ORIGINS","") or "").split(",")
CORS(app, resources={r"/*": {"origins": [o.strip() for o in ALLOWED_ORIGINS if o.strip()]}})

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

# ================== NUEVO: CHECKID + DIPOMEX (FLUJO POR API) ==================

RFC_REGEX = r"\b([A-ZÑ&]{3,4}\d{6}[A-Z0-9]{3})\b"

def extraer_rfc_solo(texto: str):
    if not texto:
        return None
    t = (texto or "").strip().upper()
    m = re.search(RFC_REGEX, t)
    return m.group(1) if m else None

def checkid_lookup(curp_or_rfc: str) -> dict:
    url = "https://www.checkid.mx/api/Busqueda"

    apikey = (os.getenv("CHECKID_APIKEY", "") or "").strip()
    timeout = int(os.getenv("CHECKID_TIMEOUT", "8") or "8")

    if not apikey:
        raise RuntimeError("CHECKID_NO_APIKEY")

    term = (curp_or_rfc or "").strip().upper()
    if not term:
        raise ValueError("CHECKID_EMPTY_TERM")

    payload = {
        "ApiKey": apikey,
        "TerminoBusqueda": term,

        # Pide SOLO lo que necesitas (menos costo si CheckID cobra por módulos)
        "ObtenerRFC": True,
        "ObtenerCURP": True,
        "ObtenerCP": True,
        "ObtenerRegimenFiscal": True,

        # Opcionales
        "ObtenerNSS": True,
        "Obtener69o69B": False,
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "CSFDocs/1.0"
    }

    r = requests.post(url, json=payload, headers=headers, timeout=timeout)

    if r.status_code == 404:
        raise RuntimeError("CHECKID_NOT_FOUND")
    if not r.ok:
        raise RuntimeError(f"CHECKID_HTTP_{r.status_code}")

    data = r.json() or {}

    # ✅ Formato real: { exitoso, error, codigoError, resultado:{...} }
    if isinstance(data, dict):
        if data.get("exitoso") is False:
            code = data.get("codigoError") or "UNKNOWN"
            raise RuntimeError(f"CHECKID_{code}")
        if data.get("error"):
            code = data.get("codigoError") or "UNKNOWN"
            raise RuntimeError(f"CHECKID_{code}")

    return data

def _norm_regimenes(reg_obj) -> list[str]:
    """
    Devuelve lista de regímenes en formato string, soporta:
      - string
      - list[str]
      - list[dict] (clave/descripcion o similares)
      - dict (a veces viene dentro)
    """
    if not reg_obj:
        return []

    raw = reg_obj.get("regimenesFiscales") if isinstance(reg_obj, dict) else reg_obj

    def to_text(item) -> str:
        if item is None:
            return ""
        # dict -> intenta varias llaves comunes
        if isinstance(item, dict):
            # ejemplos posibles: {clave:"605", descripcion:"Sueldos..."} o {codigo:"605", nombre:"..."}
            clave = item.get("clave") or item.get("codigo") or item.get("id") or ""
            desc  = item.get("descripcion") or item.get("nombre") or item.get("regimen") or ""
            clave = str(clave).strip()
            desc  = str(desc).strip()
            if clave and desc:
                return f"{clave} - {desc}"
            return desc or clave

        # string o número
        return str(item).strip()

    items = []
    if isinstance(raw, list):
        items = [to_text(x) for x in raw]
    elif isinstance(raw, dict):
        items = [to_text(raw)]
    else:
        items = [to_text(raw)]

    # limpia vacíos y duplicados (manteniendo orden)
    out = []
    seen = set()
    for x in items:
        x = x.strip()
        if not x:
            continue
        key = x.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(x)
    return out

def limpiar_regimen(regimen) -> str:
    if not regimen:
        return ""

    r = str(regimen).strip()
    r = re.sub(r"^\d{3}\s*-\s*", "", r).strip()

    r_upper = r.upper()

    if r_upper == "SIN OBLIGACIONES FISCALES":
        return r
    if r_upper == "PEMEX":
        return r
    if r.startswith("Régimen de "):
        return r

    return f"Régimen de {r}"

def _norm_checkid_fields(ci_raw: dict) -> dict:
    ci_raw = ci_raw or {}
    res = ci_raw.get("resultado") or {}

    rfc_obj = res.get("rfc") or {}
    curp_obj = res.get("curp") or {}
    cp_obj = res.get("codigoPostal") or {}
    reg_obj = res.get("regimenFiscal") or {}
    nss_obj = res.get("nss") or {}
    e69_obj = res.get("estado69o69B") or {}

    def pick(*vals):
        for v in vals:
            if v not in (None, "", []):
                return str(v).strip()
        return ""

    razon = pick(rfc_obj.get("razonSocial"))
    nombres = pick(curp_obj.get("nombres"))
    ape1 = pick(curp_obj.get("primerApellido"))
    ape2 = pick(curp_obj.get("segundoApellido"))

    if not (nombres or ape1 or ape2) and razon:
        nombres = razon

    fn_text = pick(curp_obj.get("fechaNacimientoText"))
    fn_iso = pick(curp_obj.get("fechaNacimiento"))
    fecha_nac = fn_text or fn_iso

    rfc = pick(rfc_obj.get("rfc"), rfc_obj.get("rfcRepresentante"))
    curp = pick(curp_obj.get("curp"), rfc_obj.get("curp"), rfc_obj.get("curpRepresentante"))

    cp = pick(cp_obj.get("codigoPostal"))

    # ✅ MULTI-RÉGIMEN (DOCX usa solo el primero)
    regimenes_list = _norm_regimenes(reg_obj)
    regimen_first_raw = regimenes_list[0] if regimenes_list else ""
    regimen_text = limpiar_regimen(regimen_first_raw)

    con_prob = bool(e69_obj.get("conProblema")) if isinstance(e69_obj, dict) else False
    estatus = "ACTIVO" if not con_prob else "CON_PROBLEMA_69B"

    return {
        "RFC": rfc,
        "CURP": curp,
        "CP": cp,
        "NOMBRE": nombres,
        "APELLIDO_PATERNO": ape1,
        "APELLIDO_MATERNO": ape2,

        # Para el DOCX
        "REGIMEN": regimen_text,

        # Por si lo quieres usar después (UI / tabla / logs)
        "REGIMENES": [limpiar_regimen(x) for x in regimenes_list],

        "FECHA_NACIMIENTO": fecha_nac,
        "ESTATUS": estatus,
        "NSS": pick(nss_obj.get("nss")),
        "RAZON_SOCIAL": razon,
    }

def dipomex_by_cp(cp: str) -> dict:
    """
    DIPOMEX TAU:
      GET https://api.tau.com.mx/dipomex/v1/codigo_postal?cp=09000
      Header: APIKEY: xxx
    Regresa:
      codigo_postal.estado, municipio y colonias[]
    Robust:
      - reintentos en 5xx/timeouts
      - NO truena tu flujo (no raise_for_status)
      - si respuesta no es JSON, regresa {}
    """
    apikey = (os.getenv("DIPOMEX_APIKEY", "") or "").strip()
    timeout = int(os.getenv("DIPOMEX_TIMEOUT", "12") or "12")

    # Si no hay API key, mejor no tronar el bot
    if not apikey:
        print("DIPOMEX WARN: falta DIPOMEX_APIKEY")
        return {}

    # Normaliza CP
    cp = re.sub(r"\D+", "", (cp or ""))
    if not cp:
        return {}
    cp = cp.zfill(5)

    url = "https://api.tau.com.mx/dipomex/v1/codigo_postal"
    headers = {"APIKEY": apikey, "Accept": "application/json", "User-Agent": "CSFDocs/1.0"}

    last_err = None

    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params={"cp": cp}, timeout=timeout)

            # Si es 5xx, reintenta (el servicio está fallando)
            if r.status_code >= 500:
                last_err = f"HTTP_{r.status_code}"
                print("DIPOMEX ERROR:", r.status_code, (r.text or "")[:300])
                time.sleep(0.6 * (attempt + 1))
                continue

            # 4xx (CP inválido o key), no reintenta mucho, solo log y retorna {}
            if not r.ok:
                print("DIPOMEX ERROR:", r.status_code, (r.text or "")[:600])
                return {}

            # Protege por si responde HTML aunque venga "ok"
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "application/json" not in ctype:
                # intenta json de todos modos, si falla regresa {}
                try:
                    j = r.json() or {}
                except Exception:
                    print("DIPOMEX ERROR: respuesta no JSON", (r.text or "")[:300])
                    return {}
            else:
                j = r.json() or {}

            if not isinstance(j, dict):
                return {}

            codigo_postal = j.get("codigo_postal")
            return codigo_postal if isinstance(codigo_postal, dict) else {}

        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = str(e)
            print("DIPOMEX WARN: timeout/conn", last_err)
            time.sleep(0.6 * (attempt + 1))
        except Exception as e:
            # Cualquier otra cosa: no tronar
            print("DIPOMEX ERROR: exception", repr(e))
            return {}

    print("DIPOMEX WARN: servicio no disponible", last_err)
    return {}

def _pick_first_colonia(dip: dict) -> str:
    cols = dip.get("colonias") or []
    if isinstance(cols, list) and cols:
        first = cols[0] or {}
        if isinstance(first, dict):
            return (first.get("colonia") or "").strip()
        if isinstance(first, str):
            return first.strip()
    return ""

def _det_rand_int(seed: str, lo: int, hi: int) -> int:
    """
    Aleatorio determinístico (no cambia en reintentos).
    """
    seed = (seed or "").encode("utf-8")
    h = hashlib.sha256(seed).hexdigest()
    n = int(h[:12], 16)  # suficiente
    return lo + (n % (hi - lo + 1))

def _fake_date_components(year: int, seed_key: str):
    rng = _det_rng(seed_key)
    day = rng.randint(1, 30)
    month = rng.randint(1, 12)
    return day, month, year

def _fmt_dd_de_mes_de_aaaa(day: int, month: int, year: int) -> str:
    return f"{day:02d} DE {MESES_ES[month]} DE {year}"

def _fmt_dd_mm_aaaa(day: int, month: int, year: int) -> str:
    return f"{day:02d}/{month:02d}/{year}"

def construir_datos_desde_apis(term: str) -> dict:
    term_norm = (term or "").strip().upper()
    if not term_norm:
        raise ValueError("TERM_EMPTY")

    key = f"CHECKID:{term_norm}"

    cached = cache_get(key)
    if cached:
        print("CACHE HIT:", key)
        return cached

    # ---------- 1) CheckID ----------
    ci_raw = checkid_lookup(term_norm)
    ci = _norm_checkid_fields(ci_raw)

    if not (ci.get("RFC") or ci.get("CURP")):
        raise RuntimeError("CHECKID_SIN_DATOS")

    # ---------- 2) Dipomex (SOFT FAIL) ----------
    dip = {}
    if ci.get("CP"):
        try:
            dip = dipomex_by_cp(ci["CP"]) or {}
        except Exception as e:
            print("DIPOMEX FAILED (soft):", repr(e))
            dip = {}

    # ---------- 3) Dirección + fallbacks ----------
    FALLBACK_ENTIDAD   = "CIUDAD DE MÉXICO"
    FALLBACK_MUNICIPIO = "CUAUHTÉMOC"
    FALLBACK_COLONIA   = "CENTRO"

    entidad   = (dip.get("estado") or "").strip().upper() or FALLBACK_ENTIDAD
    municipio = (dip.get("municipio") or "").strip().upper() or FALLBACK_MUNICIPIO
    colonia   = (_pick_first_colonia(dip) or "").strip().upper() or FALLBACK_COLONIA

    # Reglas fijas
    tipo_vialidad = "CALLE"
    vialidad = "SIN NOMBRE"

    # Semilla determinística
    seed_key = (ci.get("RFC") or ci.get("CURP") or term_norm).strip().upper()

    no_ext = str(_det_rand_int("NOEXT|" + seed_key, 1, 999))
    idcif_fake = str(_det_rand_int("IDCIF|" + seed_key, 10_000_000_000, 30_000_000_000))

    nombre_etiqueta = " ".join(
        x for x in [ci.get("NOMBRE"), ci.get("APELLIDO_PATERNO"), ci.get("APELLIDO_MATERNO")] if x
    ).strip()

    ahora = datetime.now(ZoneInfo("America/Mexico_City"))
    fecha_emision = _fecha_lugar_mun_ent(municipio, entidad)

    # ---------- 4) Fechas (RAW primero) ----------
    birth_year = _parse_birth_year(ci.get("FECHA_NACIMIENTO", ""))
    if birth_year:
        y = birth_year + 18
        d, m, y = _fake_date_components(y, seed_key)
        fecha_inicio_raw = _fmt_dd_de_mes_de_aaaa(d, m, y)  # puede ser "01 DE ENERO DE 2020"
        fecha_ultimo_raw = _fmt_dd_de_mes_de_aaaa(d, m, y)
        fecha_alta_raw   = _fmt_dd_mm_aaaa(d, m, y)         # puede ser "01/01/2020"
    else:
        # si NO quieres vacíos:
        fecha_inicio_raw = "01 DE ENERO DE 2000"
        fecha_ultimo_raw = "01 DE ENERO DE 2000"
        fecha_alta_raw   = "01/01/2000"

    # ---------- 5) Normalización FINAL (lo importante) ----------
    al_val = _al_from_entidad(entidad)
    
    # Fechas para PÁGINA (dd-mm-aaaa)
    fn_dash = _to_dd_mm_aaaa_dash(ci.get("FECHA_NACIMIENTO", ""))
    
    # Convierte a dash desde los RAW ya calculados en el paso 4
    # OJO: fecha_inicio_raw y fecha_ultimo_raw están en "09 DE ENERO DE 2026"
    #      fecha_alta_raw está en "09/01/2026"
    fi_dash = _to_dd_mm_aaaa_dash(fecha_inicio_raw)
    fu_dash = _to_dd_mm_aaaa_dash(fecha_ultimo_raw)
    fa_dash = _to_dd_mm_aaaa_dash(fecha_alta_raw)
    
    # --- formatos PARA DOCUMENTO ---
    fi_doc = fecha_inicio_raw          # "09 DE ENERO DE 2026"
    fu_doc = fecha_ultimo_raw          # "09 DE ENERO DE 2026"
    fa_doc = fecha_alta_raw            # "09/01/2026"
    
    # fallbacks por si algo no parseó
    if not fn_dash:
        fn_dash = "01-01-2000"
    if not fi_dash:
        fi_dash = "01-01-2018"
    if not fu_dash:
        fu_dash = fi_dash
    if not fa_dash:
        fa_dash = fi_dash

    # Régimen: si ci trae lista o string, quédate con el primero
    # (si ci["REGIMEN"] ya viene limpio, esto solo lo asegura)
    reg_val = ci.get("REGIMEN", "")
    if isinstance(reg_val, list):
        reg_val = reg_val[0] if reg_val else ""
    reg_val = limpiar_regimen(reg_val)

    datos = {
        "RFC_ETIQUETA": (ci.get("RFC") or "").strip().upper(),
        "NOMBRE_ETIQUETA": nombre_etiqueta,
        "IDCIF_ETIQUETA": idcif_fake,

        "RFC": (ci.get("RFC") or "").strip().upper(),
        "CURP": (ci.get("CURP") or "").strip().upper(),
        "NOMBRE": (ci.get("NOMBRE") or "").strip().upper(),
        "PRIMER_APELLIDO": (ci.get("APELLIDO_PATERNO") or "").strip().upper(),
        "SEGUNDO_APELLIDO": (ci.get("APELLIDO_MATERNO") or "").strip().upper(),

        "REGIMEN": reg_val,
        "ESTATUS": "ACTIVO",

        # ✅ dd-mm-aaaa
        "FECHA_INICIO": fi_dash,        # DD-MM-AAAA
        "FECHA_ULTIMO": fu_dash,        # DD-MM-AAAA
        "FECHA_ALTA": fa_dash,          # DD-MM-AAAA

        "FECHA_INICIO_DOC": fi_doc,     # 09 DE ENERO DE 2026
        "FECHA_ULTIMO_DOC": fu_doc,     # 09 DE ENERO DE 2026
        "FECHA_ALTA_DOC": fa_doc,       # 09/01/2026

        "FECHA": fecha_emision,
        "FECHA_CORTA": ahora.strftime("%Y/%m/%d %H:%M:%S"),

        "CP": (ci.get("CP") or "").strip(),

        "TIPO_VIALIDAD": tipo_vialidad,
        "VIALIDAD": vialidad,
        "NO_EXTERIOR": no_ext,
        "NO_INTERIOR": "",

        "COLONIA": colonia,
        "LOCALIDAD": municipio,
        "ENTIDAD": entidad,

        # ✅ dd-mm-aaaa
        "FECHA_NACIMIENTO": fn_dash,

        # ✅ requerido por ti
        "AL": al_val,
    }

    # ---------- 6) Cache ----------
    cache_set(key, datos)

    if datos.get("RFC"):
        cache_set(f"CHECKID:{datos['RFC'].strip().upper()}", datos)
    if datos.get("CURP"):
        cache_set(f"CHECKID:{datos['CURP'].strip().upper()}", datos)

    return datos

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
      - si contiene RFC (solo) => RFC_ONLY
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

    rfc_solo = extraer_rfc_solo(text_body or "")
    if rfc_solo:
        return "RFC_ONLY"

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

# Si quieres limitar concurrencia:
from concurrent.futures import ThreadPoolExecutor
EXEC = ThreadPoolExecutor(max_workers=3)

def safe_submit(fn, *args, **kwargs):
    try:
        EXEC.submit(fn, *args, **kwargs)
    except Exception:
        # fallback: no tumbes el webhook
        traceback.print_exc()

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
        if msg_id and wa_seen_msg(msg_id):
            print("WA DUPLICATE msg_id ignored:", msg_id)
            return "OK", 200

        contacts = value.get("contacts") or []
        raw_wa_id = (contacts[0].get("wa_id") if contacts else None) or msg.get("from")
        from_wa_id = normalizar_wa_to(raw_wa_id)
        if not from_wa_id:
            return "OK", 200

        # ✅ allow/block rápido
        try:
            st = get_state(STATS_PATH)
            from stats_store import is_allowed, is_blocked

            if not is_allowed(st, from_wa_id):
                print("WA NOT ALLOWED (ignored):", from_wa_id)
                return "OK", 200

            if is_blocked(st, from_wa_id):
                wa_send_text(from_wa_id, "⛔ Tu número está suspendido. Contacta al administrador.")
                return "OK", 200

        except Exception as e:
            print("Allow/block check error:", e)
            return "OK", 200

        # ✅ marca visto el msg_id lo antes posible (si tu wa_seen_msg usa “set”/persistencia)
        # Si tu lógica es distinta, ignora esto.
        try:
            if msg_id:
                wa_mark_seen(msg_id)  # si tienes helper; si no, omite
        except Exception:
            pass

        # ✅ dispara worker SIN bloquear
        job = {
            "from_wa_id": from_wa_id,
            "msg": msg,
            "value": value,
            "msg_id": msg_id,
            "received_at": time.time(),
        }

        # Opción A: thread simple
        # threading.Thread(target=_process_wa_message, args=(job,), daemon=True).start()

        # Opción B: pool (mejor control)
        safe_submit(_process_wa_message, job)

        return "OK", 200

    except Exception as e:
        print("Error WA webhook:", e)
        return "OK", 200

def wa_mark_seen(msg_id: str):
    if not (WA_TOKEN and WA_PHONE_NUMBER_ID and msg_id):
        return
    url = wa_api_url(f"{WA_PHONE_NUMBER_ID}/messages")
    headers = {"Authorization": f"Bearer {WA_TOKEN}", "Content-Type": "application/json"}
    payload = {"messaging_product": "whatsapp", "status": "read", "message_id": msg_id}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if not r.ok:
        print("WA MARK SEEN ERROR:", r.status_code, r.text)
        
def _process_wa_message(job: dict):
    from_wa_id = job.get("from_wa_id")
    msg = job.get("msg") or {}
    value = job.get("value") or {}
    msg_id = job.get("msg_id")

    try:
        msg_type = msg.get("type")

        text_body = ""
        image_bytes = None
        fuente_img = ""

        # 1) Parse de contenido
        if msg_type == "text":
            text_body = ((msg.get("text") or {}).get("body") or "").strip()

        elif msg_type == "image":
            media_id = ((msg.get("image") or {}).get("id") or "").strip()
            if media_id:
                media_url = wa_get_media_url(media_id)
                image_bytes = wa_download_media_bytes(media_url)

        elif msg_type == "document":
            media_id = ((msg.get("document") or {}).get("id") or "").strip()
            mime = ((msg.get("document") or {}).get("mime_type") or "")
            if media_id and (mime.startswith("image/") or mime in ("application/octet-stream", "")):
                media_url = wa_get_media_url(media_id)
                image_bytes = wa_download_media_bytes(media_url)

        # 2) Si hay imagen, intenta extraer RFC/IDCIF
        if image_bytes:
            rfc_img, idcif_img, fuente_img = extract_rfc_idcif_from_image_bytes(image_bytes)
            if rfc_img and idcif_img:
                text_body = f"RFC: {rfc_img} IDCIF: {idcif_img}"
                wa_send_text(from_wa_id, f"✅ Detecté datos por {fuente_img}.\n{rfc_img} {idcif_img}\n")
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
                return

        # 3) Si no hay nada, guía
        if not (text_body or "").strip():
            wa_send_text(
                from_wa_id,
                "📩 Envíame RFC e idCIF o una foto donde se vea el QR.\n\n"
                "Ejemplo texto:\nTOHJ640426XXX 19010347XXX"
            )
            return

        # 4) Detectar tipo de entrada (REGRA: SOLO CURP o SOLO RFC -> APIs)
        if image_bytes and (fuente_img in ("QR", "OCR")):
            input_type = "QR"
        else:
            kind, token = classify_input_for_personas(text_body)  # 👈 usa tu función
            if kind == "only_curp":
                input_type = "CURP"
            elif kind == "only_rfc":
                input_type = "RFC_ONLY"
            else:
                input_type = "RFC_IDCIF"  # todo lo demás cae aquí (incluye combos, "RFC IDCIF", etc.)

        # 5) Test mode (no cobro)
        test_mode = is_test_request(from_wa_id, text_body)

        # ✅ incrementa requests SOLO si aplica
        if input_type in ("CURP", "RFC_ONLY", "RFC_IDCIF"):
            if not test_mode:
                def _inc_req(s):
                    from stats_store import inc_request, inc_user_request
                    inc_request(s)
                    inc_user_request(s, from_wa_id)
                get_and_update(STATS_PATH, _inc_req)

        # 6) Ruteo por tipo
        if input_type in ("CURP", "RFC_ONLY"):
            query = ""
            if input_type == "CURP":
                query = (extraer_curp(text_body) or "").strip().upper()
            else:
                query = (extraer_rfc_solo(text_body) or "").strip().upper()

            if not query:
                wa_send_text(from_wa_id, "❌ No pude leer tu CURP/RFC. Intenta de nuevo.")
                return

            wa_send_text(from_wa_id, f"⏳ Generando constancia...\n{input_type}: {query}")

            datos = construir_datos_desde_apis(query)  # tu función

            # ✅ Publicar en validacion-sat para que el QR sea funcional
            try:
                pub_url = validacion_sat_publish(datos, input_type)
                if pub_url:
                    datos["QR_URL"] = pub_url
            except Exception as e:
                print("validacion_sat_publish fail:", e)
            
            _generar_y_enviar_archivos(from_wa_id, text_body, datos, input_type, test_mode)
            return

        # RFC + IDCIF (SAT)
        rfc, idcif = extraer_rfc_idcif(text_body)
        if not rfc or not idcif:
            wa_send_text(
                from_wa_id,
                "✅ Recibí tu mensaje.\n\nAhora envíame los datos en este formato:\n"
                "RFC IDCIF\n\n"
                "Tip: si quieres también Word, escribe al final: DOCX"
            )
            return

        wa_send_text(from_wa_id, f"⏳ Generando constancia...\nRFC: {rfc}\nidCIF: {idcif}")

        datos = extraer_datos_desde_sat(rfc, idcif)  # tu función
        _generar_y_enviar_archivos(from_wa_id, text_body, datos, "RFC_IDCIF", test_mode)
        return

    except Exception as e:
        print("Worker error:", e)
        traceback.print_exc()
        try:
            wa_send_text(from_wa_id, "⚠️ Ocurrió un error procesando tu solicitud. Intenta de nuevo.")
        except Exception:
            pass

def _pick(*vals) -> str:
    for v in vals:
        if v not in (None, "", [], {}):
            return str(v).strip()
    return ""

def _upper(s: str) -> str:
    return (s or "").strip().upper()

def _digits(s: str) -> str:
    return re.sub(r"\D+", "", (s or ""))

def norm_persona_from_datos(datos: dict, rfc: str, idcif: str, d3_key: str) -> dict:
    datos = datos or {}

    # ---- base fields (elige de ambas variantes) ----
    curp   = _upper(_pick(datos.get("CURP"), datos.get("curp")))
    nombre = _upper(_pick(datos.get("NOMBRE"), datos.get("nombre")))
    ap1    = _upper(_pick(datos.get("PRIMER_APELLIDO"), datos.get("apellido_paterno")))
    ap2    = _upper(_pick(datos.get("SEGUNDO_APELLIDO"), datos.get("apellido_materno")))

    # ---- fechas dd-mm-aaaa SIEMPRE ----
    fn = _to_dd_mm_aaaa_dash(_pick(datos.get("FECHA_NACIMIENTO"), datos.get("fecha_nacimiento")))
    fi = _to_dd_mm_aaaa_dash(_pick(datos.get("FECHA_INICIO"), datos.get("fecha_inicio_operaciones")))
    fu = _to_dd_mm_aaaa_dash(_pick(datos.get("FECHA_ULTIMO"), datos.get("fecha_ultimo_cambio")))
    fa = _to_dd_mm_aaaa_dash(_pick(datos.get("FECHA_ALTA"), datos.get("fecha_alta")))

    # ---- direccion uppercase ----
    entidad   = _upper(_pick(datos.get("ENTIDAD"), datos.get("entidad")))
    municipio = _upper(_pick(datos.get("LOCALIDAD"), datos.get("municipio")))
    colonia   = _upper(_pick(datos.get("COLONIA"), datos.get("colonia")))

    tipo_v = _upper(_pick(datos.get("TIPO_VIALIDAD"), datos.get("tipo_vialidad"), "CALLE")) or "CALLE"
    nom_v  = _upper(_pick(datos.get("VIALIDAD"), datos.get("nombre_vialidad"), "SIN NOMBRE")) or "SIN NOMBRE"

    no_ext = _digits(_pick(datos.get("NO_EXTERIOR"), datos.get("numero_exterior")))
    no_int = _digits(_pick(datos.get("NO_INTERIOR"), datos.get("numero_interior")))
    cp     = _digits(_pick(datos.get("CP"), datos.get("cp")))

    # ---- regimen limpio ----
    reg = _pick(datos.get("REGIMEN"), datos.get("regimen"))
    reg = limpiar_regimen(reg)  # usa tu limpiar_regimen actual

    # ---- estatus ----
    estatus = _upper(_pick(datos.get("ESTATUS"), datos.get("situacion_contribuyente"), "ACTIVO")) or "ACTIVO"

    # ---- AL = "ENTIDAD 1" ----
    al = _al_from_entidad(entidad)

    # ---- etiquetas ----
    nombre_etiqueta = " ".join(x for x in [nombre, ap1, ap2] if x).strip()

    return {
        "D1": "10",
        "D2": "1",
        "D3": d3_key,

        "rfc": _upper(rfc),
        "curp": curp,
        "nombre": nombre,
        "apellido_paterno": ap1,
        "apellido_materno": ap2,

        "fecha_nacimiento": fn,
        "fecha_inicio_operaciones": fi,
        "situacion_contribuyente": estatus,
        "fecha_ultimo_cambio": fu,
        "regimen": reg,
        "fecha_alta": fa,

        "entidad": entidad,
        "municipio": municipio,
        "colonia": colonia,
        "tipo_vialidad": tipo_v,
        "nombre_vialidad": nom_v,
        "numero_exterior": no_ext,
        "numero_interior": no_int,
        "cp": cp,

        "correo": _pick(datos.get("CORREO"), datos.get("correo")),
        "al": al,

        "RFC_ETIQUETA": _upper(rfc),
        "NOMBRE_ETIQUETA": nombre_etiqueta,
        "IDCIF_ETIQUETA": str(idcif).strip(),
    }
    
def _generar_y_enviar_archivos(from_wa_id: str, text_body: str, datos: dict, input_type: str, test_mode: bool):
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # ✅ Guardar en GitHub personas.json SOLO si viene de APIs
    if input_type in ("CURP", "RFC_ONLY"):
        idcif = datos.get("IDCIF") or datos.get("IDCIF_ETIQUETA")

        if not idcif:
            raise RuntimeError("❌ Falta IDCIF fakey en datos (IDCIF / IDCIF_ETIQUETA)")

        rfc = (datos.get("RFC") or datos.get("rfc") or "").strip().upper()
        if not rfc:
            raise RuntimeError("❌ Falta RFC en datos (RFC / rfc)")

        d3_key = f"{idcif}_{rfc}"

        persona = norm_persona_from_datos(datos=datos, rfc=rfc, idcif=idcif, d3_key=d3_key)

        try:
            github_update_personas(d3_key, persona)
        except Exception as e:
            print("⚠ Error actualizando personas.json:", e)
    
    reg = (datos.get("REGIMEN") or "").upper()
    if ("SUELDOS" in reg) and ("SALARIOS" in reg):
        nombre_plantilla = "plantilla-asalariado.docx"
    else:
        nombre_plantilla = "plantilla.docx"

    ruta_plantilla = os.path.join(base_dir, nombre_plantilla)

    t_upper = (text_body or "").upper()
    quiere_docx = ("DOCX" in t_upper) or ("WORD" in t_upper) or ("AMBOS" in t_upper)

    with tempfile.TemporaryDirectory() as tmpdir:
        nombre_base = (datos.get("CURP") or datos.get("RFC") or "CONSTANCIA").strip() or "CONSTANCIA"
        nombre_docx = f"{nombre_base}_{input_type}.docx"
        ruta_docx = os.path.join(tmpdir, nombre_docx)

        reemplazar_en_documento(ruta_plantilla, ruta_docx, datos, input_type)

        with open(ruta_docx, "rb") as f:
            docx_bytes = f.read()

        # PDF default
        try:
            pdf_path = os.path.join(tmpdir, os.path.splitext(nombre_docx)[0] + ".pdf")
            docx_to_pdf_aspose(docx_path=ruta_docx, pdf_path=pdf_path)

            with open(pdf_path, "rb") as f:
                pdf_bytes = f.read()

            pdf_filename = os.path.splitext(nombre_docx)[0] + ".pdf"

            media_pdf = wa_upload_document(pdf_bytes, pdf_filename, "application/pdf")
            wa_send_document(from_wa_id, media_pdf, pdf_filename)

            _bill_and_log_ok(from_wa_id, input_type, datos, test_mode)

            if quiere_docx:
                media_docx = wa_upload_document(
                    docx_bytes,
                    nombre_docx,
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                )
                wa_send_document(from_wa_id, media_docx, nombre_docx, caption="📄 (Opcional) También te dejo el Word (DOCX).")

        except Exception as e:
            print("PDF fail, sending DOCX fallback:", e)

            media_docx = wa_upload_document(
                docx_bytes,
                nombre_docx,
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
            wa_send_document(from_wa_id, media_docx, nombre_docx, caption="⚠️ No pude convertir a PDF, pero aquí está en DOCX.")

            _bill_and_log_ok(from_wa_id, input_type, datos, test_mode)

def _bill_and_log_ok(from_wa_id: str, input_type: str, datos: dict, test_mode: bool):
    # Usa tus helpers: resolve_price, make_ok_key, bill_success_if_new, log_attempt, inc_success
    out = {"reason": None}

    def _tx(s):
        from stats_store import bill_success_if_new, log_attempt, resolve_price, inc_success

        price_mxn = resolve_price(s, from_wa_id, input_type)
        ok_key = make_ok_key(input_type, datos.get("RFC"), datos.get("CURP"))

        res = bill_success_if_new(
            s,
            user=from_wa_id,
            ok_key=ok_key,
            input_type=input_type,
            price_mxn=price_mxn,
            is_test=test_mode
        )

        out["reason"] = res.get("reason")
        if res.get("billed"):
            inc_success(s, from_wa_id, (datos.get("RFC") or ""))
            log_attempt(s, from_wa_id, ok_key, True, "BILLED_OK",
                        {"via": "WA", "type": input_type, "price": price_mxn}, is_test=test_mode)
        else:
            code = "OK_NO_BILL"
            if res.get("reason") == "DUPLICATE":
                code = "OK_DUPLICATE_NO_BILL"
            elif res.get("reason") == "TEST":
                code = "OK_TEST_NO_BILL"
            log_attempt(s, from_wa_id, ok_key, True, code,
                        {"via": "WA", "type": input_type, "reason": res.get("reason")}, is_test=test_mode)

    get_and_update(STATS_PATH, _tx)

    if out["reason"] == "DUPLICATE":
        wa_send_text(from_wa_id, "⚠️ Este trámite ya fue generado antes. No se cobrará de nuevo.")

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
    curp = (request.form.get("curp") or "").strip().upper()
    lugar_emision = (request.form.get("lugar_emision") or "").strip()

    # ✅ Decide flujo:
    # - si viene CURP => APIs
    # - si viene RFC sin IDCIF => APIs
    # - si viene RFC+IDCIF => SAT (como siempre)
    input_type = None
    term = None

    if curp:
        input_type = "CURP"
        term = curp
    elif rfc and not idcif:
        input_type = "RFC_ONLY"
        term = rfc
    elif rfc and idcif:
        input_type = "RFC_IDCIF"
    else:
        return jsonify({"ok": False, "message": "Falta RFC/IDCIF o CURP."}), 400

    try:
        if input_type in ("CURP", "RFC_ONLY"):
            datos = construir_datos_desde_apis(term)

            # ✅ publicar para QR funcional
            try:
                pub_url = validacion_sat_publish(datos, input_type)
                if pub_url:
                    datos["QR_URL"] = pub_url
            except Exception as e:
                print("validacion_sat_publish fail:", e)

        else:
            datos = extraer_datos_desde_sat(rfc, idcif)

    except ValueError as e:
        if str(e) == "SIN_DATOS_SAT":
            return jsonify({"ok": False, "message": "No se encontró información en el SAT para ese RFC / idCIF."}), 404
        print("Error datos:", e)
        return jsonify({"ok": False, "message": "Error consultando datos."}), 500
    except Exception as e:
        print("Error consultando datos:", e)
        return jsonify({"ok": False, "message": "Error consultando datos."}), 500

    if lugar_emision:
        hoy = hoy_mexico()
        dia = f"{hoy.day:02d}"
        mes = MESES_ES[hoy.month]
        anio = hoy.year
        datos["FECHA"] = f"{lugar_emision.upper()} A {dia} DE {mes} DE {anio}"

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Elegir plantilla según el régimen
    reg = (datos.get("REGIMEN") or "").upper()
    if ("SUELDOS" in reg) and ("SALARIOS" in reg):
        nombre_plantilla = "plantilla-asalariado.docx"
    else:
        nombre_plantilla = "plantilla.docx"

    ruta_plantilla = os.path.join(base_dir, nombre_plantilla)

    with tempfile.TemporaryDirectory() as tmpdir:
        nombre_base = datos.get("CURP") or rfc or "CONSTANCIA"
        nombre_docx = f"{nombre_base}_{input_type}.docx"
        ruta_docx = os.path.join(tmpdir, nombre_docx)

        reemplazar_en_documento(ruta_plantilla, ruta_docx, datos, input_type)

        # ====== STATS: success (bill + log) ======
        def _inc_ok(s):
            from stats_store import bill_success_if_new, log_attempt, resolve_price, inc_success
        
            price_mxn = resolve_price(s, user, input_type)
        
            ok_key = make_ok_key(input_type, datos.get("RFC"), datos.get("CURP"))
        
            res = bill_success_if_new(
                s,
                user=user,
                ok_key=ok_key,
                input_type=input_type,
                price_mxn=price_mxn,
                is_test=test_mode
            )
        
            if res.get("billed"):
                inc_success(s, user, (datos.get("RFC") or ""))
                log_attempt(
                    s, user, ok_key, True, "BILLED_OK",
                    {"via": "WEB", "type": input_type, "price": price_mxn},
                    is_test=test_mode
                )
            else:
                code = "OK_NO_BILL"
                if res.get("reason") == "DUPLICATE":
                    code = "OK_DUPLICATE_NO_BILL"
                elif res.get("reason") == "TEST":
                    code = "OK_TEST_NO_BILL"
        
                log_attempt(
                    s, user, ok_key, True, code,
                    {"via": "WEB", "type": input_type, "reason": res.get("reason")},
                    is_test=test_mode
                )
        
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
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403
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
    wa_id = re.sub(r"\D+", "", (data.get("wa_id") or ""))
    
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
    wa_id = re.sub(r"\D+", "", (data.get("wa_id") or ""))

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
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    # si mandas { "keep_allowlist": false } entonces sí la borra
    data = request.get_json(silent=True) or {}
    keep_allowlist = bool(data.get("keep_allowlist", True))
    keep_blocklist = bool(data.get("keep_blocklist", True))

    def _reset(state: dict):
        # backup de listas antes de limpiar
        allow_enabled = bool(state.get("allowlist_enabled") or False)
        allow_wa = list(state.get("allowlist_wa") or [])
        allow_meta = dict(state.get("allowlist_meta") or {})

        blocked_wa = list(state.get("blocked_wa") or [])
        blocked_meta = dict(state.get("blocked_meta") or {})

        state.clear()

        # estructura mínima “en blanco”
        state.update({
            "request_total": 0,
            "success_total": 0,
            "por_dia": {},
            "por_usuario": {},
            "last_success": [],
            "attempts": {},
            "rfc_ok_index": {},
            "billing": {
                "price_mxn": float(PRICE_PER_OK_MXN or 0),
                "total_billed": 0,
                "total_revenue_mxn": 0.0,
                "by_user": {}
            },
            # siempre deja la bandera (aunque no guardes lista)
            "allowlist_enabled": allow_enabled if keep_allowlist else False,
        })

        # ✅ restaurar allowlist si se pidió mantener
        if keep_allowlist:
            state["allowlist_wa"] = allow_wa
            state["allowlist_meta"] = allow_meta

        # ✅ restaurar bloqueados si se pidió mantener
        if keep_blocklist:
            state["blocked_wa"] = blocked_wa
            state["blocked_meta"] = blocked_meta

    get_and_update(STATS_PATH, _reset)

    return jsonify({
        "ok": True,
        "message": "Reset aplicado",
        "keep_allowlist": keep_allowlist,
        "keep_blocklist": keep_blocklist
    })

@app.route("/admin/wa/allow/add", methods=["POST"])
def admin_wa_allow_add():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = re.sub(r"\D+", "", (data.get("wa_id") or ""))
    note = (data.get("note") or "").strip()

    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import allow_add, log_attempt
        allow_add(s, wa_id, note=note)
        log_attempt(s, wa_id, None, True, "ALLOW_ADDED", {"note": note}, is_test=False)

    st = get_and_update(STATS_PATH, _do)

    # devuelve lo que hay realmente ya guardado
    merged = sorted(set(st.get("allowlist_wa") or []) | set((st.get("allowlist_meta") or {}).keys()))
    return jsonify({"ok": True, "wa_id": wa_id, "allowed": True, "count": len(merged)})

@app.route("/admin/wa/allow/remove", methods=["POST"])
def admin_wa_allow_remove():
    if ADMIN_STATS_TOKEN:
        t = request.headers.get("X-Admin-Token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    data = request.get_json(silent=True) or {}
    wa_id = re.sub(r"\D+", "", (data.get("wa_id") or ""))
    if not wa_id:
        return jsonify({"ok": False, "message": "Falta wa_id"}), 400

    def _do(s):
        from stats_store import allow_remove, log_attempt
        allow_remove(s, wa_id)
        log_attempt(s, wa_id, None, True, "ALLOW_REMOVED", {}, is_test=False)

    st = get_and_update(STATS_PATH, _do)
    merged = sorted(set(st.get("allowlist_wa") or []) | set((st.get("allowlist_meta") or {}).keys()))
    return jsonify({"ok": True, "wa_id": wa_id, "allowed": False, "count": len(merged)})

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

    allow_wa = s.get("allowlist_wa") or []
    allow_meta = s.get("allowlist_meta") or {}

    merged = sorted(set(allow_wa) | set(allow_meta.keys()))

    return jsonify({
        "ok": True,
        "allowlist_enabled": bool(s.get("allowlist_enabled") or False),
        "allowlist_wa": merged,
        "allowlist_meta": allow_meta,
        "count": len(merged),
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

@app.route("/admin/wa/block/list", methods=["GET"])
def admin_wa_block_list():
    if ADMIN_STATS_TOKEN:
        t = request.args.get("token", "")
        if t != ADMIN_STATS_TOKEN:
            return jsonify({"ok": False, "message": "Forbidden"}), 403

    s = get_state(STATS_PATH)

    blocked_wa = s.get("blocked_wa") or []
    blocked_meta = s.get("blocked_meta") or {}

    # 🔧 por si alguna vez se guardó solo en meta o solo en lista:
    merged = sorted(set(blocked_wa) | set(blocked_meta.keys()))

    return jsonify({
        "ok": True,
        "blocked_wa": merged,
        "blocked_meta": blocked_meta,
        "count": len(merged),
    })


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
          --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
          --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
          --ok:#22c55e;
          --warn:#f59e0b;
          --bad:#ef4444;
          --accent:#7c3aed;
          --accent2:#60a5fa;
        }
    
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
    
        .wrap{max-width:1180px;margin:0 auto;padding:18px 16px 28px}
        .topbar{
          position:sticky;top:0;z-index:5;
          backdrop-filter: blur(12px);
          background: linear-gradient(to bottom, rgba(11,16,32,.85), rgba(11,16,32,.55));
          border-bottom:1px solid rgba(255,255,255,.08);
        }
        .topbarInner{max-width:1180px;margin:0 auto;padding:14px 16px;display:flex;gap:14px;align-items:center;justify-content:space-between}
        .brand{display:flex;gap:12px;align-items:center}
        .logo{
          width:40px;height:40px;border-radius:14px;
          background: linear-gradient(135deg, rgba(124,58,237,.95), rgba(96,165,250,.85));
          box-shadow: 0 10px 24px rgba(124,58,237,.25);
          display:flex;align-items:center;justify-content:center;
          font-weight:800;
        }
        .title{display:flex;flex-direction:column;line-height:1.05}
        .title b{font-size:15px}
        .title span{font-size:12px;color:var(--muted)}
    
        .chips{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
        .chip{
          display:inline-flex;align-items:center;gap:8px;
          padding:8px 10px;border-radius:999px;
          background:rgba(255,255,255,.06);
          border:1px solid rgba(255,255,255,.10);
          font-size:12px;color:var(--muted);
          max-width: 100%;
          overflow:hidden;text-overflow:ellipsis;white-space:nowrap;
        }
        .dot{width:8px;height:8px;border-radius:999px;background:var(--accent2)}
        .dot.ok{background:var(--ok)}
        .dot.warn{background:var(--warn)}
    
        .grid{
          display:grid;
          grid-template-columns:repeat(12, 1fr);
          gap:12px;
          margin-top:14px;
        }
    
        .card{
          background: linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.05));
          border:1px solid rgba(255,255,255,.10);
          border-radius:var(--radius);
          box-shadow:var(--shadow);
          padding:14px;
          overflow:hidden;
        }
        .cardHeader{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
        .cardHeader h2{margin:0;font-size:13px;color:var(--muted);font-weight:600;letter-spacing:.2px}
        .kpi{display:flex;gap:10px;align-items:flex-end}
        .big{font-size:34px;font-weight:900;letter-spacing:-.6px}
        .sub{font-size:12px;color:var(--muted2);margin-top:4px}
        .mono{font-family:var(--mono)}
    
        .kpiCard{grid-column:span 4}
        .wide{grid-column:span 7}
        .side{grid-column:span 5}
    
        @media (max-width: 920px){
          .kpiCard{grid-column:span 6}
          .wide{grid-column:span 12}
          .side{grid-column:span 12}
          .topbarInner{flex-direction:column;align-items:flex-start}
          .chips{justify-content:flex-start}
        }
        @media (max-width: 560px){
          .kpiCard{grid-column:span 12}
          .big{font-size:32px}
        }
    
        .pill{
          font-size:12px;
          padding:6px 10px;
          border-radius:999px;
          background:rgba(124,58,237,.12);
          border:1px solid rgba(124,58,237,.30);
          color:rgba(232,236,255,.95);
          display:inline-flex;align-items:center;gap:8px;
        }
    
        .bar{height:10px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);overflow:hidden}
        .barFill{height:100%;border-radius:999px;background:linear-gradient(90deg, rgba(34,197,94,.95), rgba(96,165,250,.85))}
    
        .tableWrap{
          border:1px solid rgba(255,255,255,.10);
          border-radius:16px;
          overflow:hidden;
          background:rgba(0,0,0,.10);
        }
        table{width:100%;border-collapse:separate;border-spacing:0}
        thead th{
          position:sticky;top:0;z-index:2;
          text-align:left;
          font-size:12px;
          color:rgba(232,236,255,.78);
          background:rgba(11,16,32,.80);
          backdrop-filter: blur(10px);
          border-bottom:1px solid rgba(255,255,255,.10);
          padding:10px 12px;
          letter-spacing:.2px;
        }
        tbody td{
          padding:10px 12px;
          border-bottom:1px solid rgba(255,255,255,.08);
          font-size:13px;
          color:rgba(232,236,255,.92);
          vertical-align:top;
        }
        tbody tr:nth-child(odd) td{background:rgba(255,255,255,.02)}
        tbody tr:hover td{background:rgba(96,165,250,.06)}
        .num{text-align:right;font-variant-numeric: tabular-nums}
        .empty{padding:14px;color:var(--muted);text-align:center}
    
        .scroll{max-height:420px;overflow:auto}
        .scroll::-webkit-scrollbar{height:10px;width:10px}
        .scroll::-webkit-scrollbar-thumb{background:rgba(255,255,255,.12);border-radius:999px}
        .scroll::-webkit-scrollbar-track{background:rgba(255,255,255,.05)}
    
        .userCell{display:flex;gap:10px;align-items:center}
        .avatar{
          width:36px;height:36px;border-radius:14px;
          background:linear-gradient(135deg, rgba(124,58,237,.85), rgba(96,165,250,.70));
          display:flex;align-items:center;justify-content:center;
          font-weight:900;
        }
        .userMeta{display:flex;flex-direction:column;line-height:1.1;min-width:0}
        .userName{font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:260px}
    
        .chipsBox{display:flex;gap:8px;flex-wrap:wrap}
        .chip.mono{color:rgba(232,236,255,.92)}
    
        .footer{margin-top:14px;color:var(--muted2);font-size:12px;display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}
        a{color:rgba(96,165,250,.9);text-decoration:none}
        a:hover{text-decoration:underline}
    
        .btn{
          padding:10px 12px;
          border-radius:12px;
          border:1px solid rgba(255,255,255,.14);
          background:rgba(255,255,255,.08);
          color:var(--text);
          cursor:pointer;
          font-weight:700;
          font-size:13px;
        }
        .btn:hover{ background:rgba(255,255,255,.10); }
        .btn:active{ transform: translateY(1px); }
    
        .btn.danger{
          background: rgba(239,68,68,.14);
          border-color: rgba(239,68,68,.28);
        }
        .btn.warn{
          background: rgba(245,158,11,.14);
          border-color: rgba(245,158,11,.28);
        }
    
        /* ====== ADDON: billing visual + modal + search ====== */
        .actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
        .input{
          padding:10px 12px;border-radius:12px;
          border:1px solid rgba(255,255,255,.14);
          background:rgba(0,0,0,.18);
          color:var(--text);
          outline:none;
          width:min(420px, 100%);
        }
        .miniBar{height:9px;border-radius:999px;background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.10);overflow:hidden}
        .miniFill{height:100%;border-radius:999px;background:linear-gradient(90deg, rgba(124,58,237,.95), rgba(96,165,250,.85));width:0%}
    
        .modalMask{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:center;justify-content:center;padding:18px;z-index:50}
        .modal{
          width:min(920px, 100%);border-radius:18px;
          border:1px solid rgba(255,255,255,.12);
          background:linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.05));
          box-shadow:0 18px 60px rgba(0,0,0,.45);
          overflow:hidden;
        }
        .modalHead{display:flex;align-items:center;justify-content:space-between;padding:14px;border-bottom:1px solid rgba(255,255,255,.10)}
        .modalBody{padding:14px}
        .modalBody pre{
          margin:0;padding:12px;border-radius:14px;
          background:rgba(0,0,0,.22);border:1px solid rgba(255,255,255,.10);
          overflow:auto;max-height:55vh;color:rgba(232,236,255,.92);white-space:pre-wrap
        }
        .mutedSmall{font-size:12px;color:var(--muted2)}
    
        /* =========================
           ✅ Acciones rápidas (layout pro por filas)
           ========================= */
        
        .quickGrid{
          display:grid;
          grid-template-columns: repeat(12, 1fr);
          gap:12px;
          align-items:stretch;
          margin-top:10px;
        }
        
        /* Cards */
        .qCard{
          border:1px solid rgba(255,255,255,.10);
          background:rgba(0,0,0,.14);
          border-radius:16px;
          padding:12px;
          box-shadow:none;
          min-height: 160px; /* uniforma altura visual */
        }
        
        .qCard h3{
          margin:0 0 10px 0;
          font-size:13px;
          color:rgba(232,236,255,.90);
          letter-spacing:.2px;
          display:flex;
          align-items:center;
          justify-content:space-between;
        }
        
        .qTag{
          font-size:11px;
          padding:5px 8px;
          border-radius:999px;
          background:rgba(255,255,255,.06);
          border:1px solid rgba(255,255,255,.10);
          color:rgba(232,236,255,.70);
        }
        
        .stack{display:flex;flex-direction:column;gap:8px}
        .row{display:flex;gap:8px;flex-wrap:wrap}
        .row .btn{flex:1 1 160px}
        
        /* inputs dentro de quick actions (más compactos) */
        .quickGrid .input{
          padding:9px 11px;
          border-radius:12px;
          width:100%;
        }
        
        /* ===== Layout por spans (desktop) ===== */
        .q-wa{ grid-column: span 3; }
        .q-web{ grid-column: span 3; }
        .q-allow{ grid-column: span 3; }
        .q-pricing{ grid-column: span 3; }
        
        .q-billing{ grid-column: span 8; }
        .q-critical{ grid-column: span 4; }
        
        /* Zona crítica */
        .dangerZone{
          border:1px solid rgba(239,68,68,.35);
          background: linear-gradient(180deg, rgba(239,68,68,.10), rgba(0,0,0,.10));
        }
        
        /* ===== Tablet ===== */
        @media (max-width: 920px){
          .q-wa, .q-web, .q-allow, .q-pricing{ grid-column: span 6; }
          .q-billing, .q-critical{ grid-column: span 12; }
        }
        
        /* ===== Móvil ===== */
        @media (max-width: 560px){
          .q-wa, .q-web, .q-allow, .q-pricing,
          .q-billing, .q-critical{ grid-column: span 12; }
          .qCard{ min-height: unset; }
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
            <div class="sub">Solicitudes totales registrados en el sistema.</div>
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
              <div class="qCard q-wa">
                <h3>📱 WhatsApp <span class="qTag">Bloqueo / Unblock</span></h3>
                <div class="stack">
                  <div class="sub">WA ID (ej: 52xxxxxxxxxx)</div>
                  <input id="waId" class="input" placeholder="52899..." />
                  <input id="waReason" class="input" placeholder="Motivo (opcional)" />
                  <div class="row">
                    <button class="btn danger" onclick="blockWA()">Bloquear</button>
                    <button class="btn" onclick="unblockWA()">Desbloquear</button>
                  </div>
                </div>
              </div>
            
              <!-- Web -->
              <div class="qCard q-web">
                <h3>🌐 Web <span class="qTag">Sesiones</span></h3>
                <div class="stack">
                  <div class="sub">Usuario WEB (username)</div>
                  <input id="webUser" class="input" placeholder="graciela.barajas" />
                  <div class="row">
                    <button class="btn warn" onclick="kickWeb()">Kick sesión</button>
                    <button class="btn" onclick="openUser()">Ver stats</button>
                  </div>
                  <div class="mutedSmall">Tip: “Kick” fuerza cierre de sesión en backend.</div>
                </div>
              </div>
            
              <!-- Permisos / Allowlist -->
                <div class="qCard q-allow">
                  <h3>✅ Permisos <span class="qTag">Allowlist</span></h3>
                  <div class="stack">
                    <div class="sub">WA ID para permitir/quitar</div>
                    <input id="allowId" class="input" placeholder="52899..." />
                    <input id="allowNote" class="input" placeholder="Nota (opcional)" />
                
                    <div class="row">
                      <button class="btn" onclick="allowAdd()">Permitir</button>
                      <button class="btn warn" onclick="allowRemove()">Quitar</button>
                    </div>
                
                    <div class="row">
                      <button class="btn" onclick="allowToggle(true)">Activar</button>
                      <button class="btn danger" onclick="allowToggle(false)">Desactivar</button>
                    </div>
                
                    <div class="row">
                      <button class="btn" onclick="viewAllowlist()">Ver allowlist</button>
                      <button class="btn" onclick="viewBlocked()">Ver bloqueados</button>
                    </div>
                
                    <div class="mutedSmall">Este campo es independiente del módulo “WhatsApp”.</div>
                  </div>
                </div>

              <!-- Pricing -->
              <div class="qCard q-pricing">
                <h3>💲 Precios <span class="qTag">Por usuario</span></h3>
                <div class="stack">
                  <div class="sub">Usuario (WA o username)</div>
                  <input id="pUser" class="input" placeholder="52899... o graciela.barajas" />
            
                  <div class="row">
                    <select id="pType" class="input" style="flex:1 1 180px">
                      <option value="RFC_IDCIF">RFC + IDCIF</option>
                      <option value="QR">QR (foto)</option>
                      <option value="CURP">CURP</option>
                      <option value="RFC_ONLY">RFC</option>
                    </select>
                    <input id="pPrice" class="input" placeholder="70" style="flex:1 1 120px" />
                  </div>
            
                  <div class="row">
                    <button class="btn" onclick="setUserPrice()">Guardar</button>
                    <button class="btn warn" onclick="delUserPrice()">Borrar</button>
                  </div>
                  <button class="btn" onclick="openPricing()">Ver pricing JSON</button>
                </div>
              </div>
            
              <!-- Datos / Billing -->
              <div class="qCard q-billing">
                <h3>💳 Datos / Billing <span class="qTag">Admin</span></h3>
                <div class="stack">
                  <div class="sub">RFC a borrar (deduplicación + facturación)</div>
                  <div class="row">
                    <input id="rfcDel" class="input" placeholder="VAEC9409082X6" style="flex:2 1 260px" />
                    <button class="btn warn" onclick="deleteRFC()" style="flex:1 1 180px">Borrar RFC</button>
                  </div>
            
                  <div class="sub" style="margin-top:4px">Consultas</div>
                  <div class="row">
                    <button class="btn" onclick="openBilling()">Facturación global</button>
                    <button class="btn" onclick="openBillingUser()">Facturación por usuario</button>
                  </div>
                </div>
              </div>
            
              <!-- Zona crítica -->
              <div class="qCard q-critical dangerZone">
                <h3>⚠️ Zona crítica <span class="qTag">Irreversible</span></h3>
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
              <span class="sub">Solicitudes · OK · Tasa</span>
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
            elHint.textContent = price > 0 ? "Precio activo y ganancia calculándose." : "⚠️ PRICE_PER_OK_MXN está en 0 (revenue siempre será 0).";
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
            
                const billingUser = await fetch("/admin/billing/user/" + encodeURIComponent(user) + q, { cache:"no-store" })
                  .then(r=>r.json());
            
                const statsUser = await fetch("/admin/okrfcs/" + encodeURIComponent(user) + q, { cache:"no-store" })
                  .then(r=>r.json())
                  .catch(()=> ({}));
            
                showModal(
                  "Detalle: " + user,
                  "billing + ok_rfcs",
                  { billingUser, statsUser }
                );
              }catch(e){
                out({ ok:false, error:e });
              }
            }
            
            function showModal(title, sub, obj){
              document.getElementById("mTitle").textContent = title || "Detalle";
              document.getElementById("mSub").textContent = sub || "";
              document.getElementById("mPre").textContent = JSON.stringify(obj || {}, null, 2);
              document.getElementById("mask").style.display = "flex";
            }
            
            function closeModal(ev){
              document.getElementById("mask").style.display = "none";
            }
            
            async function openJson(kind){
              const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
              const path = kind === "billing" ? ("/admin/billing" + q) : ("/stats" + q);
              window.open(path, "_blank");
            }
            
            // ====== Allowlist helpers que sí estás llamando pero NO existen aún ======
            function allowId(){ return (document.getElementById("allowId").value || "").trim(); }
            function allowNote(){ return (document.getElementById("allowNote").value || "").trim(); }
            
            async function allowAdd(){
              try{
                const id = allowId();
                if(!id) return out("Falta WA ID");
                const data = await api("/admin/wa/allow/add", "POST", { wa_id: id, note: allowNote() });
                out(data);
              }catch(e){ out(e); }
            }
            async function allowRemove(){
              try{
                const id = allowId();
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
            async function viewAllowlist(){
              try{
                const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
                const data = await fetch("/admin/wa/allow/list" + q, { cache:"no-store" }).then(r=>r.json());
                showModal("Allowlist", "list", data);
              }catch(e){ out(e); }
            }
            async function viewBlocked(){
              try{
                const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
                const data = await fetch("/admin/wa/block/list" + q, { cache:"no-store" }).then(r=>r.json());
                showModal("Bloqueados", "list", data);
              }catch(e){ out(e); }
            }
            
            // ====== Pricing helpers que también estás llamando pero faltan ======
            function pUser(){ return (document.getElementById("pUser").value || "").trim(); }
            function pType(){ return (document.getElementById("pType").value || "").trim(); }
            function pPrice(){ return Number((document.getElementById("pPrice").value || "0").trim()); }
            
            async function setUserPrice(){
              try{
                const u = pUser();
                if(!u) return out("Falta usuario");
                const data = await api("/admin/pricing/user/set", "POST", { user:u, type:pType(), price_mxn:pPrice() });
                out(data);
                reloadBilling();
              }catch(e){ out(e); }
            }
            async function delUserPrice(){
              try{
                const u = pUser();
                if(!u) return out("Falta usuario");
                const data = await api("/admin/pricing/user/delete", "POST", { user:u, type:pType() });
                out(data);
                reloadBilling();
              }catch(e){ out(e); }
            }
            function openPricing(){
              const q = ADMIN_TOKEN ? ("?token=" + encodeURIComponent(ADMIN_TOKEN)) : "";
              window.open("/admin/pricing" + q, "_blank");
            }
            
            // carga inicial
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


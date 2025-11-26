# -*- coding: utf-8 -*-
import os
import re
import ssl
import io
import tempfile
import zipfile
from datetime import date, datetime
from io import BytesIO

import base64
from xhtml2pdf import pisa

import qrcode
import requests
from barcode import Code128
from barcode.writer import ImageWriter
from bs4 import BeautifulSoup
from docx import Document
from flask import Flask, request, send_file, abort
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from zipfile import ZipFile

# ================== ADAPTADOR TLS SAT ==================

class SATAdapter(HTTPAdapter):
    """
    Adaptador para forzar un contexto TLS que no use DH de clave pequeña.
    """
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        # Preferir cifrados fuertes pero sin DH
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

# ================== FUNCIONES AUXILIARES ==================

def formatear_fecha_dd_de_mmmm_de_aaaa(d_str, sep="-"):
    """
    Recibe una fecha tipo '12-06-1987' y regresa
    '12 DE JUNIO DE 1987'
    """
    if not d_str:
        return ""
    partes = d_str.strip().split(sep)
    if len(partes) != 3:
        return d_str  # si no coincide, regresa tal cual
    dd, mm, yyyy = partes
    try:
        dia = int(dd)
        mes = int(mm)
        anio = int(yyyy)
    except ValueError:
        return d_str
    nombre_mes = MESES_ES.get(mes, mm)
    return f"{dia} DE {nombre_mes} DE {anio}"

def fecha_actual_lugar(localidad, entidad):
    """
    Construye FECHA como:
    'LOCALIDAD , ENTIDAD A 26 DE NOVIEMBRE DE 2025'
    usando la fecha de hoy.
    """
    hoy = date.today()
    dia = hoy.day
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

    # --- Código de barras desde TEC-IT ---
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
    """
    Regresa un diccionario:
    {
      'Nombre:': 'JUAN FRANCISCO',
      'Apellido Paterno:': 'TORRES',
      ...
    }
    tomando los TR con dos TD.
    """
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
    """
    Pega al validador móvil del SAT usando RFC + idCIF
    y extrae los campos necesarios.
    """
    d3 = f"{idcif}_{rfc}"

    url = "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
    params = {
        "D1": "10",
        "D2": "1",
        "D3": d3
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

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

    fecha_actual = fecha_actual_lugar(localidad, entidad)

    hoy = date.today()
    fecha_corta = f"{hoy.day:02d}/{hoy.month:02d}/{hoy.year}"

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
    """
    Reemplaza placeholders en el DOCX y actualiza QR/código de barras.
    """
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
            elif item.filename == "word/media/image7.png":
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

# ================== AQUI DEFINIMOS LA APP FLASK ==================

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Backend OK. Usa POST /generar desde el formulario."

@app.route("/generar", methods=["POST"])
def generar_constancia():
    # 1) Leer datos del formulario
    rfc = (request.form.get("rfc") or "").strip().upper()
    idcif = (request.form.get("idcif") or "").strip()

    if not rfc or not idcif:
        return abort(400, "Falta RFC o idCIF")

    # 2) Consultar SAT
    try:
        datos = extraer_datos_desde_sat(rfc, idcif)
    except Exception as e:
        print("Error consultando SAT:", e)
        return abort(500, "Error consultando SAT o extrayendo datos")

    # 3) Generar QR y código de barras (para el PDF)
    d3 = f"{idcif}_{rfc}"
    url_qr = (
        "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
        f"?D1=10&D2=1&D3={d3}"
    )

    try:
        qr_bytes, barcode_bytes = generar_qr_y_barcode(url_qr, rfc)
    except Exception as e:
        print("Error generando QR/código de barras:", e)
        # Si falla el QR, seguimos sin imágenes
        qr_bytes = None
        barcode_bytes = None

    qr_b64 = base64.b64encode(qr_bytes).decode("ascii") if qr_bytes else ""
    barcode_b64 = base64.b64encode(barcode_bytes).decode("ascii") if barcode_bytes else ""

    # 4) Construir HTML sencillo de la constancia
    html = f"""
    <html>
    <head>
      <meta charset="UTF-8" />
      <style>
        body {{
          font-family: DejaVu Sans, Arial, sans-serif;
          font-size: 11pt;
        }}
        .container {{
          width: 90%;
          max-width: 700px;
          margin: 0 auto;
        }}
        h1 {{
          text-align: center;
          font-size: 16pt;
          margin-bottom: 12px;
        }}
        h2 {{
          font-size: 13pt;
          margin-top: 18px;
          margin-bottom: 6px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
          margin-bottom: 10px;
        }}
        td, th {{
          border: 1px solid #999;
          padding: 4px 6px;
        }}
        .label {{
          width: 35%;
          font-weight: bold;
          background: #f2f2f2;
        }}
        .imgs {{
          margin-top: 20px;
          text-align: center;
        }}
        .imgs img {{
          margin: 6px 10px;
        }}
        .small {{
          font-size: 9pt;
          color: #555;
        }}
      </style>
    </head>
    <body>
      <div class="container">
        <h1>CONSTANCIA DE SITUACIÓN FISCAL</h1>

        <p><strong>RFC:</strong> {datos.get("RFC", "")}</p>
        <p><strong>Nombre:</strong> {datos.get("NOMBRE_ETIQUETA", "")}</p>
        <p><strong>Fecha de emisión:</strong> {datos.get("FECHA_CORTA", "")}</p>

        <h2>Datos de identificación</h2>
        <table>
          <tr><td class="label">RFC</td><td>{datos.get("RFC", "")}</td></tr>
          <tr><td class="label">CURP</td><td>{datos.get("CURP", "")}</td></tr>
          <tr><td class="label">Nombre</td><td>{datos.get("NOMBRE", "")}</td></tr>
          <tr><td class="label">Primer apellido</td><td>{datos.get("PRIMER_APELLIDO", "")}</td></tr>
          <tr><td class="label">Segundo apellido</td><td>{datos.get("SEGUNDO_APELLIDO", "")}</td></tr>
          <tr><td class="label">Fecha inicio de operaciones</td><td>{datos.get("FECHA_INICIO", "")}</td></tr>
          <tr><td class="label">Estatus en el padrón</td><td>{datos.get("ESTATUS", "")}</td></tr>
          <tr><td class="label">Fecha último cambio de situación</td><td>{datos.get("FECHA_ULTIMO", "")}</td></tr>
        </table>

        <h2>Domicilio fiscal</h2>
        <table>
          <tr><td class="label">Código Postal</td><td>{datos.get("CP", "")}</td></tr>
          <tr><td class="label">Vialidad</td><td>{datos.get("TIPO_VIALIDAD", "")} {datos.get("VIALIDAD", "")}</td></tr>
          <tr><td class="label">Número exterior</td><td>{datos.get("NO_EXTERIOR", "")}</td></tr>
          <tr><td class="label">Número interior</td><td>{datos.get("NO_INTERIOR", "")}</td></tr>
          <tr><td class="label">Colonia</td><td>{datos.get("COLONIA", "")}</td></tr>
          <tr><td class="label">Localidad / Municipio</td><td>{datos.get("LOCALIDAD", "")}</td></tr>
          <tr><td class="label">Entidad federativa</td><td>{datos.get("ENTIDAD", "")}</td></tr>
        </table>

        <h2>Régimen fiscal</h2>
        <table>
          <tr><td class="label">Régimen</td><td>{datos.get("REGIMEN", "")}</td></tr>
          <tr><td class="label">Fecha de alta en el régimen</td><td>{datos.get("FECHA_ALTA", "")}</td></tr>
        </table>

        <p class="small">{datos.get("FECHA", "")}</p>

        <div class="imgs">
          {"<img src='data:image/png;base64," + qr_b64 + "' width='120' />" if qr_b64 else ""}
          {"<img src='data:image/png;base64," + barcode_b64 + "' height='60' />" if barcode_b64 else ""}
        </div>

        <p class="small">
          Este documento es una representación impresa de la constancia electrónica
          consultada mediante el servicio del SAT.
        </p>
      </div>
    </body>
    </html>
    """

    # 5) Convertir HTML a PDF en memoria
    pdf_buffer = io.BytesIO()
    pisa_status = pisa.CreatePDF(html, dest=pdf_buffer)

    if pisa_status.err:
        print("Error generando PDF:", pisa_status.err)
        return abort(500, "Hubo un problema al generar el PDF de la constancia.")

    pdf_buffer.seek(0)

    nombre_base = datos.get("CURP") or rfc or "CONSTANCIA"

    return send_file(
        pdf_buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"{nombre_base}_RFC.pdf"
    )

if __name__ == "__main__":
    app.run(debug=True, port=5000)



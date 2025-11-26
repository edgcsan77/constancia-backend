# -*- coding: utf-8 -*-
import requests, ssl, re, qrcode, urllib.parse, os
from docx2pdf import convert
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
from bs4 import BeautifulSoup
from datetime import date, datetime
from docx import Document
from zipfile import ZipFile
from io import BytesIO
from barcode import Code128
from barcode.writer import ImageWriter

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
    # Si no hay localidad/entidad, podrías forzar algo tipo "CIUDAD DE MÉXICO"
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
    # Construir parámetro D3 = idCIF_RFC (ajusta si tu QR usa otro formato)
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

    # 👉 Usar sesión con adaptador TLS especial
    session = requests.Session()
    session.mount("https://siat.sat.gob.mx", SATAdapter())

    try:
        resp = session.get(url, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
    except requests.exceptions.SSLError as e:
        print("\n[ERROR SSL] Problema con el cifrado del servidor del SAT:")
        print(e)
        print("Revisa si la página abre bien en tu navegador. "
              "Si sigue fallando, el SAT puede haber cambiado su configuración.")
        raise

    soup = BeautifulSoup(resp.text, "html.parser")
    mapa = obtener_mapa_trs(soup)

    # Helper para obtener valor por etiqueta (versión flexible)
    def get_val(*keys_posibles):
        for k in keys_posibles:
            if k in mapa:
                return mapa[k]
        return ""

    # Datos de nombre
    nombre = get_val("Nombre:", "Nombre (s):")
    ape1 = get_val("Apellido Paterno:", "Primer Apellido:")
    ape2 = get_val("Apellido Materno:", "Segundo Apellido:")
    nombre_etiqueta = " ".join(x for x in [nombre, ape1, ape2] if x).strip()

    # Fechas
    fecha_inicio_raw = get_val("Fecha de Inicio de operaciones:", "Fecha inicio de operaciones:")
    fecha_ultimo_raw = get_val("Fecha del último cambio de situación:", "Fecha de último cambio de estado:")

    fecha_inicio_texto = formatear_fecha_dd_de_mmmm_de_aaaa(fecha_inicio_raw, sep="-")
    fecha_ultimo_texto = formatear_fecha_dd_de_mmmm_de_aaaa(fecha_ultimo_raw, sep="-")

    # Estatus
    estatus = get_val("Situación del contribuyente:", "Estatus en el padrón:")

    # CURP (si existe)
    curp = get_val("CURP:")

    # Domicilio
    cp = get_val("CP:", "Código Postal:")
    tipo_vialidad = get_val("Tipo de vialidad:")
    vialidad = get_val("Nombre de la vialidad:")
    no_ext = get_val("Número exterior:")
    no_int = get_val("Número interior:")
    colonia = get_val("Colonia:", "Nombre de la Colonia:")
    localidad = get_val("Municipio o delegación:", "Nombre del Municipio o Demarcación Territorial:")
    entidad = get_val("Entidad Federativa:", "Nombre de la Entidad Federativa:")

    # Régimen (toma el primero)
    regimen = get_val("Régimen:")

    # Fecha de alta (dd/mm/aaaa)
    fecha_alta_raw = get_val("Fecha de alta:")
    if fecha_alta_raw:
        fecha_alta = fecha_alta_raw.replace("-", "/")
    else:
        fecha_alta = ""

    # FECHA con lugar + fecha actual
    fecha_actual = fecha_actual_lugar(localidad, entidad)

    # FECHA CORTA: dd/mm/aaaa
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
    1) Reemplaza placeholders {{ ... }} en los XML del DOCX (document, headers, footers).
    2) Sustituye las imágenes del QR y código de barras por versiones generadas
       con el RFC/idCIF actuales.
    3) Segundo pase con python-docx para atrapar cualquier {{ ... }} que haya
       quedado suelto (sin pelear con los runs).
    """
    # ------------ 0) Preparar URL y bytes de QR/código de barras ------------
    rfc_val = datos.get("RFC_ETIQUETA") or datos.get("RFC", "")
    idcif_val = datos.get("IDCIF_ETIQUETA", "")

    d3 = f"{idcif_val}_{rfc_val}"
    url_qr = (
        "https://siat.sat.gob.mx/app/qr/faces/pages/mobile/validadorqr.jsf"
        f"?D1=10&D2=1&D3={d3}"
    )

    qr_bytes, barcode_bytes = generar_qr_y_barcode(url_qr, rfc_val)

    # ------------ 1) Reemplazo a nivel XML (texto) ------------
    placeholders = {
        # Etiquetas de portada
        "{{ RFC ETIQUETA }}": datos.get("RFC_ETIQUETA", ""),
        "{{ NOMBRE ETIQUETA }}": datos.get("NOMBRE_ETIQUETA", ""),
        "{{ idCIF }}": datos.get("IDCIF_ETIQUETA", ""),

        # Fechas
        "{{ FECHA }}": datos.get("FECHA", ""),
        "{{ FECHA CORTA }}": datos.get("FECHA_CORTA", ""),

        # Identificación
        "{{ RFC }}": datos.get("RFC", ""),
        "{{ CURP }}": datos.get("CURP", ""),
        "{{ NOMBRE }}": datos.get("NOMBRE", ""),
        "{{ PRIMER APELLIDO }}": datos.get("PRIMER_APELLIDO", ""),
        "{{ SEGUNDO APELLIDO }}": datos.get("SEGUNDO_APELLIDO", ""),
        "{{ FECHA INICIO }}": datos.get("FECHA_INICIO", ""),
        "{{ ESTATUS }}": datos.get("ESTATUS", ""),
        "{{ FECHA ULTIMO }}": datos.get("FECHA_ULTIMO", ""),

        # Domicilio
        "{{ CP }}": datos.get("CP", ""),
        "{{ TIPO VIALIDAD }}": datos.get("TIPO_VIALIDAD", ""),
        "{{ VIALIDAD }}": datos.get("VIALIDAD", ""),
        "{{ NO EXTERIOR }}": datos.get("NO_EXTERIOR", ""),
        "{{ NO INTERIOR }}": datos.get("NO_INTERIOR", ""),
        "{{ COLONIA }}": datos.get("COLONIA", ""),
        "{{ LOCALIDAD }}": datos.get("LOCALIDAD", ""),
        "{{ ENTIDAD }}": datos.get("ENTIDAD", ""),

        # Régimen
        "{{ REGIMEN }}": datos.get("REGIMEN", ""),
        "{{ FECHA ALTA }}": datos.get("FECHA_ALTA", ""),
    }

    with ZipFile(ruta_entrada, "r") as zin, ZipFile(ruta_salida, "w") as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)

            # 1a) Texto visible en XML
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
                    # --- parche especial para el placeholder de idCIF ---
                    idcif_val = datos.get("IDCIF_ETIQUETA", "")
                    if idcif_val:
                        # Busca cualquier secuencia {{ ... idCIF ... }}
                        # aunque esté partida en varios <w:t>
                        patron_idcif = r"<w:t>{{</w:t>.*?<w:t>idCIF</w:t>.*?<w:t>}}</w:t>"
                        xml_text, _ = re.subn(
                            patron_idcif,
                            f"<w:t>{idcif_val}</w:t>",
                            xml_text,
                            flags=re.DOTALL
                        )

                    # Reemplazos normales de placeholders de texto
                    for k, v in placeholders.items():
                        if k in xml_text:
                            xml_text = xml_text.replace(k, v)

                    data = xml_text.encode("utf-8")


            # 1b) Sustituir imágenes de QR y código de barras
            if item.filename == "word/media/image2.png":
                data = qr_bytes
            elif item.filename == "word/media/image7.png":
                data = barcode_bytes

            zout.writestr(item, data)

    # ------------ 2) Segundo pase con python-docx ------------
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

def main():
    print("=== Generador de Constancia SAT desde plantilla.docx ===")
    rfc = input("Ingresa RFC: ").strip().upper()
    idcif = input("Ingresa idCIF: ").strip()

    print("\nConsultando SAT...")
    datos = extraer_datos_desde_sat(rfc, idcif)

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # ----- NOMBRES DE ARCHIVOS -----
    nombre_docx = f"{datos['CURP']}_RFC.docx"
    nombre_pdf  = f"{datos['CURP']}_RFC.pdf"

    ruta_plantilla = os.path.join(base_dir, "plantilla.docx")
    ruta_docx      = os.path.join(base_dir, nombre_docx)
    ruta_pdf       = os.path.join(base_dir, nombre_pdf)

    print("Llenando plantilla.docx...")
    reemplazar_en_documento(ruta_plantilla, ruta_docx, datos)

    print("Convirtiendo a PDF...")
    convert(ruta_docx, ruta_pdf)

    print("\n✅ LISTO TODO:")
    print("DOCX:", ruta_docx)
    print("PDF :", ruta_pdf)
    print("Ábrelo y verifica que todo esté correcto.")

if __name__ == "__main__":
    main()

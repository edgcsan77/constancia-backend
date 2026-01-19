# static_persona.py
# -*- coding: utf-8 -*-
import os
import random
from datetime import datetime
from zoneinfo import ZoneInfo

import core_sat as core

MESES_ES = {
    1: "ENERO", 2: "FEBRERO", 3: "MARZO", 4: "ABRIL",
    5: "MAYO", 6: "JUNIO", 7: "JULIO", 8: "AGOSTO",
    9: "SEPTIEMBRE", 10: "OCTUBRE", 11: "NOVIEMBRE", 12: "DICIEMBRE",
}

def hoy_mexico():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).date()
    except Exception:
        return datetime.utcnow().date()

def fecha_actual_lugar(localidad, entidad):
    hoy = hoy_mexico()
    dia = str(hoy.day).zfill(2)
    mes = MESES_ES[hoy.month]
    anio = hoy.year
    loc = (localidad or "").upper()
    ent = (entidad or "").upper()
    if loc and ent:
        pref = f"{loc} , {ent} A "
    elif loc:
        pref = f"{loc} A "
    elif ent:
        pref = f"{ent} A "
    else:
        pref = ""
    return f"{pref}{dia} DE {mes} DE {anio}"

def build_registro_static(
    *,
    datos_curp: dict,
    rfc: str,
    curp: str,
    regimen: str = None,
    fecha_unica_dd_mm_aaaa: str = None,
    modo_domicilio: str = "auto",
    ruta_sepomex: str = "sepomex.csv",
):
    """
    Arma el 'registro' en el mismo formato que tu personas.json/plantilla.
    - datos_curp: dict con nombre/apellidos/fecha_nac_str/entidad_registro/municipio_registro
    - rfc: RFC proporcionado por usuario (NO lo calculamos aquí)
    """
    rfc = (rfc or "").strip().upper()
    curp = (curp or "").strip().upper()

    # Fechas base
    fecha_nac, fecha_inicio_operaciones = core.generar_fechas(datos_curp["fecha_nac_str"])
    fecha_ultimo_cambio = fecha_inicio_operaciones

    fecha_nac_str_out = core.formatear_dd_mm_aaaa(fecha_nac)
    fecha_inicio_str_out = core.formatear_dd_mm_aaaa(fecha_inicio_operaciones)
    fecha_ultimo_str_out = core.formatear_dd_mm_aaaa(fecha_ultimo_cambio)
    fecha_alta = fecha_inicio_str_out

    regimen_out = regimen or core.REGIMEN

    # Override fechas (si te lo pasan manual en dd-mm-aaaa)
    if fecha_unica_dd_mm_aaaa:
        fecha_unica_dd_mm_aaaa = fecha_unica_dd_mm_aaaa.strip().replace("/", "-")
        fecha_inicio_str_out = fecha_unica_dd_mm_aaaa
        fecha_ultimo_str_out = fecha_unica_dd_mm_aaaa
        fecha_alta = fecha_unica_dd_mm_aaaa

    # Domicilio
    dom_entidad = datos_curp["entidad_registro"]
    dom_municipio = datos_curp["municipio_registro"]

    if modo_domicilio == "manual_validado":
        # aquí tú podrías pasar un dict de domicilio manual ya validado
        # (para API no conviene pedir input(); eso era para consola)
        raise RuntimeError("manual_validado requiere que le pases domicilio ya validado (API).")
    else:
        direccion = core.generar_direccion_real(
            dom_entidad, dom_municipio, ruta_sepomex=ruta_sepomex, permitir_fallback=True
        )

    # CIF + D3
    cif_num = random.randint(10_000_000_000, 30_000_000_000)
    cif_str = str(cif_num)
    D1, D2 = "10", "1"
    D3 = f"{cif_str}_{rfc}"

    registro = {
        "D1": D1,
        "D2": D2,
        "D3": D3,

        "rfc": rfc,
        "curp": curp,
        "nombre": datos_curp["nombre"],
        "apellido_paterno": datos_curp["apellido_paterno"],
        "apellido_materno": datos_curp["apellido_materno"],
        "fecha_nacimiento": fecha_nac_str_out,
        "fecha_inicio_operaciones": fecha_inicio_str_out,
        "situacion_contribuyente": core.SITUACION_CONTRIBUYENTE,
        "fecha_ultimo_cambio": fecha_ultimo_str_out,
        "regimen": regimen_out,
        "fecha_alta": fecha_alta,

        "entidad": core.formatear_entidad_salida(dom_entidad),
        "municipio": dom_municipio,
        "colonia": direccion["colonia"],
        "tipo_vialidad": direccion["tipo_vialidad"],
        "nombre_vialidad": direccion["nombre_vialidad"],
        "numero_exterior": direccion["numero_exterior"],
        "numero_interior": direccion["numero_interior"],
        "cp": direccion["cp"],
        "correo": "",
        "al": "",
    }

    return registro

def build_url_qr(D1: str, D2: str, D3: str):
    url_base = "https://siat.sat.validacion-sat.com/app/qr/faces/pages/mobile/validadorqr.jsf"
    return f"{url_base}?D1={D1}&D2={D2}&D3={D3}"

def build_placeholders_for_docx(registro: dict, url_qr: str):
    # Convierte a placeholders (lo mismo que hacías en tu script)
    rfc = registro["rfc"]
    nombre = registro["nombre"]
    ape1 = registro["apellido_paterno"]
    ape2 = registro["apellido_materno"]
    nombre_etiqueta = " ".join(x for x in [nombre, ape1, ape2] if x).strip()

    d3 = registro["D3"]
    cif = d3.split("_", 1)[0] if "_" in d3 else d3

    # fecha lugar/corta
    fecha_larga = fecha_actual_lugar(registro["municipio"], registro["entidad"])
    ahora = datetime.now(ZoneInfo("America/Mexico_City"))
    fecha_corta = ahora.strftime("%Y/%m/%d %H:%M:%S")

    return {
        "URL_QR": url_qr,
        "RFC_ETIQUETA": rfc,
        "NOMBRE_ETIQUETA": nombre_etiqueta,
        "IDCIF_ETIQUETA": cif,

        "RFC": rfc,
        "CURP": registro["curp"],
        "NOMBRE": nombre,
        "PRIMER_APELLIDO": ape1,
        "SEGUNDO_APELLIDO": ape2,
        "FECHA_LUGAR": fecha_larga,
        "FECHA_CORTA": fecha_corta,

        "FECHA_INICIO": registro["fecha_inicio_operaciones"],
        "FECHA_ULTIMO": registro["fecha_ultimo_cambio"],
        "FECHA_ALTA": (registro.get("fecha_alta") or "").replace("-", "/"),

        "ESTATUS": registro["situacion_contribuyente"],
        "CP": registro["cp"],
        "TIPO_VIALIDAD": registro["tipo_vialidad"],
        "VIALIDAD": registro["nombre_vialidad"],
        "NO_EXTERIOR": registro["numero_exterior"],
        "NO_INTERIOR": registro["numero_interior"],
        "COLONIA": registro["colonia"],
        "LOCALIDAD": registro["municipio"],
        "ENTIDAD": registro["entidad"],
        "REGIMEN": registro["regimen"],
    }

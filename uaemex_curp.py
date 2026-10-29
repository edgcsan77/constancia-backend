# -*- coding: utf-8 -*-

"""
SYS RFC RENDER - UAEMEX CURP V1

Proveedor secundario de identidad CURP.

Endpoint:
https://nuevoingreso.uaemex.mx/curp.php

Metodo: POST
Content-Type: application/x-www-form-urlencoded

No registra aspirantes.
No modifica bases de datos.
No genera documentos.
"""

import os
import re
import logging
from datetime import datetime

import requests


logger = logging.getLogger(__name__)

BASE_URL = "https://nuevoingreso.uaemex.mx"

API_URL = BASE_URL + "/curp.php"

CURP_PATTERN = re.compile(
    r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2}$"
)

ENTIDADES = {
    "AS", "BC", "BS", "CC", "CL", "CM",
    "CS", "CH", "DF", "DG", "GT", "GR",
    "HG", "JC", "MC", "MN", "MS", "NT",
    "NL", "OC", "PL", "QT", "QR", "SP",
    "SL", "SR", "TC", "TS", "TL", "VZ",
    "YN", "ZS", "NE",
}


class UaemexError(Exception):
    pass


def _timeout():

    try:
        value = float(
            os.getenv("UAEMEX_CURP_TIMEOUT", "15")
        )

        return max(3.0, min(value, 30.0))

    except (ValueError, TypeError):
        return 15.0


def consultar_curp_uaemex(curp: str) -> dict:

    curp = str(curp or "").strip().upper()

    if not CURP_PATTERN.fullmatch(curp):
        raise UaemexError(
            "UAEMEX_CURP_INVALIDA"
        )

    timeout = _timeout()

    masked = (
        curp[:4]
        + "..."
        + curp[-4:]
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/130.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html, */*; q=0.01",
        "Content-Type": (
            "application/x-www-form-urlencoded; charset=UTF-8"
        ),
        "X-Requested-With": "XMLHttpRequest",
        "Origin": BASE_URL,
        "Referer": BASE_URL + "/posgrado/registro",
    }

    try:

        response = requests.post(
            API_URL,
            data={
                "curp": curp,
            },
            headers=headers,
            timeout=timeout,
        )

        if response.status_code != 200:
            raise UaemexError(
                f"UAEMEX_HTTP_{response.status_code}"
            )

        raw = (
            response.text or ""
        ).strip()

        if not raw:
            raise UaemexError(
                "UAEMEX_EMPTY_RESPONSE"
            )

        # Formato observado:
        #
        # 1|AN|CURP|PATERNO|MATERNO|NOMBRE|
        # SEXO|FECHA|PAIS|ENTIDAD|NUM_ENTIDAD|
        # MUNICIPIO|MENSAJE

        parts = raw.split("|")

        if len(parts) < 13:
            raise UaemexError(
                "UAEMEX_INVALID_RESPONSE"
            )

        status = parts[0].strip()
        codigo = parts[1].strip().upper()

        returned_curp = (
            parts[2].strip().upper()
        )

        paterno = (
            parts[3].strip().upper()
        )

        materno = (
            parts[4].strip().upper()
        )

        nombre = (
            parts[5].strip().upper()
        )

        sexo = (
            parts[6].strip().upper()
        )

        fecha = parts[7].strip()

        pais = (
            parts[8].strip().upper()
        )

        entidad = (
            parts[9].strip().upper()
        )

        num_entidad = (
            parts[10].strip()
        )

        municipio = (
            parts[11].strip()
        )

        mensaje = "|".join(
            parts[12:]
        ).strip()

        # ====================================================
        # VALIDACIONES
        # ====================================================

        if status != "1":
            raise UaemexError(
                "UAEMEX_NOT_FOUND"
            )

        if returned_curp != curp:
            raise UaemexError(
                "UAEMEX_CURP_MISMATCH"
            )

        if not nombre or not paterno:
            raise UaemexError(
                "UAEMEX_DATA_INCOMPLETE"
            )

        try:
            nacimiento = datetime.strptime(
                fecha,
                "%d/%m/%Y"
            )

        except ValueError as error:
            raise UaemexError(
                "UAEMEX_INVALID_BIRTHDATE"
            ) from error

        if (
            nacimiento.strftime("%y%m%d")
            != curp[4:10]
        ):
            raise UaemexError(
                "UAEMEX_CURP_DATE_MISMATCH"
            )

        if sexo not in ("H", "M"):
            raise UaemexError(
                "UAEMEX_SEX_UNKNOWN"
            )

        if sexo != curp[10]:
            raise UaemexError(
                "UAEMEX_CURP_SEX_MISMATCH"
            )

        if entidad != curp[11:13]:
            raise UaemexError(
                "UAEMEX_CURP_ENTITY_MISMATCH"
            )

        if entidad not in ENTIDADES:
            raise UaemexError(
                "UAEMEX_ENTITY_INVALID"
            )

        # ====================================================
        # NORMALIZAR PARA CORE_SAT
        # ====================================================

        identity = {
            "CURP": curp,

            "NOMBRE": nombre,

            "PRIMER_APELLIDO": paterno,

            "SEGUNDO_APELLIDO": materno,

            "FECHA_NACIMIENTO":
                nacimiento.strftime("%d-%m-%Y"),

            "SEXO": sexo,

            "CVE_ENTIDAD_NACIMIENTO": entidad,

            "NUM_ENTIDAD_REGISTRO": num_entidad,

            "CVE_MUNICIPIO_REGISTRO": municipio,

            "PAIS_NACIMIENTO": pais,

            "ENTIDAD_REGISTRO": "",

            "MUNICIPIO_REGISTRO": "",

            "UAEMEX_CODIGO": codigo,

            "UAEMEX_MENSAJE": mensaje,

            "SOURCE": "UAEMEX_CURP",

            "CURP_SOURCE": "UAEMEX_CURP",
        }

        logger.info(
            "UAEMEX_CURP_OK curp=%s",
            masked
        )

        return identity

    except UaemexError:
        raise

    except requests.Timeout as error:
        raise UaemexError(
            "UAEMEX_TIMEOUT"
        ) from error

    except requests.RequestException as error:
        raise UaemexError(
            "UAEMEX_REQUEST_ERROR:"
            + type(error).__name__
        ) from error

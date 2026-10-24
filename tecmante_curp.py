# -*- coding: utf-8 -*-

"""
SYS RFC RENDER - TEC MANTE CURP V1

Proveedor secundario de identidad CURP.

Orden previsto:
SIURP -> TEC MANTE -> SEQ -> NL -> FGR

No registra aspirantes.
No modifica bases de datos.
No genera documentos.
"""

import os
import re
import json
import html
import logging

from datetime import datetime
from html.parser import HTMLParser
from urllib.parse import unquote

import requests


logger = logging.getLogger(__name__)

BASE_URL = "https://inscripciones.tecmante.edu.mx/"

API_URL = (
    "https://inscripciones.tecmante.edu.mx/"
    "livewire/message/inscripcion.preinscripcion"
)

CURP_PATTERN = re.compile(
    r"^[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2}$"
)


class TecManteError(Exception):
    pass


class LivewireParser(HTMLParser):

    def __init__(self):
        super().__init__()
        self.components = []

    def handle_starttag(self, tag, attrs):

        attrs = dict(attrs)
        initial = attrs.get("wire:initial-data")

        if not initial:
            return

        try:
            component = json.loads(
                html.unescape(initial)
            )

            name = (
                component
                .get("fingerprint", {})
                .get("name")
            )

            if name == "inscripcion.preinscripcion":
                self.components.append(component)

        except (ValueError, TypeError, AttributeError):
            pass


def _timeout():

    try:
        value = float(
            os.getenv("TECMANTE_CURP_TIMEOUT", "20")
        )

        return max(3.0, min(value, 30.0))

    except (ValueError, TypeError):
        return 20.0


def consultar_curp_tecmante(curp: str) -> dict:

    curp = str(curp or "").strip().upper()

    if not CURP_PATTERN.fullmatch(curp):
        raise TecManteError("TECMANTE_CURP_INVALIDA")

    timeout = _timeout()

    masked = curp[:4] + "..." + curp[-4:]

    session = requests.Session()

    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml",
    })

    try:

        # 1. OBTENER SESION LIVEWIRE

        response = session.get(
            BASE_URL,
            timeout=timeout
        )

        response.raise_for_status()

        parser = LivewireParser()
        parser.feed(response.text)

        if not parser.components:
            raise TecManteError(
                "TECMANTE_COMPONENT_NOT_FOUND"
            )

        component = parser.components[0]

        fingerprint = component.get("fingerprint")
        server_memo = component.get("serverMemo")

        if not fingerprint or not server_memo:
            raise TecManteError(
                "TECMANTE_LIVEWIRE_STATE_INVALID"
            )

        if not server_memo.get("checksum"):
            raise TecManteError(
                "TECMANTE_CHECKSUM_MISSING"
            )

        # 2. CONSULTA CURP

        payload = {
            "fingerprint": fingerprint,
            "serverMemo": server_memo,
            "updates": [
                {
                    "type": "syncInput",
                    "payload": {
                        "id": "curp-sync",
                        "name": "curp",
                        "value": curp
                    }
                },
                {
                    "type": "callMethod",
                    "payload": {
                        "id": "curp-consulta",
                        "method": "buscacurp",
                        "params": []
                    }
                }
            ]
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": BASE_URL.rstrip("/"),
            "Referer": BASE_URL,
        }

        xsrf = session.cookies.get("XSRF-TOKEN")

        if xsrf:
            headers["X-XSRF-TOKEN"] = unquote(xsrf)

        response = session.post(
            API_URL,
            json=payload,
            headers=headers,
            timeout=timeout
        )

        if response.status_code != 200:
            raise TecManteError(
                f"TECMANTE_HTTP_{response.status_code}"
            )

        try:
            result = response.json()

        except ValueError as error:
            raise TecManteError(
                "TECMANTE_INVALID_JSON"
            ) from error

        if not isinstance(result, dict):
            raise TecManteError(
                "TECMANTE_INVALID_RESPONSE"
            )

        memo = result.get("serverMemo") or {}

        if memo.get("errors"):
            raise TecManteError(
                "TECMANTE_LIVEWIRE_ERRORS"
            )

        data = memo.get("data") or {}

        if not isinstance(data, dict):
            raise TecManteError(
                "TECMANTE_DATA_INVALID"
            )

        # 3. NORMALIZAR IDENTIDAD

        nombre = str(
            data.get("nombre") or ""
        ).strip().upper()

        paterno = str(
            data.get("paterno") or ""
        ).strip().upper()

        materno = str(
            data.get("materno") or ""
        ).strip().upper()

        fecha = str(
            data.get("fecha_nacimiento") or ""
        ).strip()

        sexo = str(
            data.get("sexo") or ""
        ).strip().upper()

        entidad = str(
            data.get("estado_nacimiento") or ""
        ).strip().upper()

        if not nombre or not paterno or not fecha:
            raise TecManteError(
                "TECMANTE_DATA_INCOMPLETE"
            )

        try:
            nacimiento = datetime.strptime(
                fecha,
                "%Y-%m-%d"
            )

        except ValueError as error:
            raise TecManteError(
                "TECMANTE_INVALID_BIRTHDATE"
            ) from error

        if nacimiento.strftime("%y%m%d") != curp[4:10]:
            raise TecManteError(
                "TECMANTE_CURP_DATE_MISMATCH"
            )

        if sexo in ("MUJER", "FEMENINO", "M"):
            sexo_curp = "M"

        elif sexo in ("HOMBRE", "MASCULINO", "H"):
            sexo_curp = "H"

        else:
            raise TecManteError(
                "TECMANTE_SEX_UNKNOWN"
            )

        if sexo_curp != curp[10]:
            raise TecManteError(
                "TECMANTE_CURP_SEX_MISMATCH"
            )

        if not entidad or entidad != curp[11:13]:
            raise TecManteError(
                "TECMANTE_CURP_ENTITY_MISMATCH"
            )

        # Formato esperado por core_sat.py:
        # DD-MM-YYYY

        fecha_normalizada = nacimiento.strftime(
            "%d-%m-%Y"
        )

        identity = {
            "CURP": curp,
            "NOMBRE": nombre,
            "PRIMER_APELLIDO": paterno,
            "SEGUNDO_APELLIDO": materno,
            "FECHA_NACIMIENTO": fecha_normalizada,
            "SEXO": sexo_curp,
            "CVE_ENTIDAD_NACIMIENTO": entidad,

            # No confundir nacimiento con registro.
            "ENTIDAD_REGISTRO": "",
            "MUNICIPIO_REGISTRO": "",

            "SOURCE": "TECMANTE_CURP",
            "CURP_SOURCE": "TECMANTE_CURP",
        }

        logger.info(
            "TECMANTE_CURP_OK curp=%s",
            masked
        )

        return identity

    except TecManteError:
        raise

    except requests.Timeout as error:
        raise TecManteError(
            "TECMANTE_TIMEOUT"
        ) from error

    except requests.RequestException as error:
        raise TecManteError(
            "TECMANTE_REQUEST_ERROR:"
            + type(error).__name__
        ) from error

    finally:
        session.close()

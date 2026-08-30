from __future__ import annotations

import re

TERMINAL_CODES = {
    "IDCIF_INVALID",
    "RFC_DADO_DE_BAJA",
    "RFC_CANCELADO",
    "RFC_INACTIVO",
    "RFC_SIN_ESTATUS",
}

REASON_LABELS = {
    "IDCIF_INVALID": "ID INCORRECTO",
    "RFC_DADO_DE_BAJA": "RFC DADO DE BAJA",
    "RFC_CANCELADO": "RFC CANCELADO",
    "RFC_INACTIVO": "RFC INACTIVO",
    "RFC_SIN_ESTATUS": "RFC SIN ESTATUS",
}


def normalize_status(value: str | None) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").strip().upper(),
    )


def classify_not_eligible_error(error_text: str | None) -> str:
    text = normalize_status(error_text)

    if text.startswith("CLIENT_RFC_STATUS_MISSING"):
        return "RFC_SIN_ESTATUS"

    if text.startswith("CLIENT_RFC_WITHOUT_REGIMEN"):
        # LOTES permite sin régimen; si aparece aquí es inconsistencia temporal.
        return "SAT_TEMPORAL_ERROR"

    if text.startswith("CLIENT_RFC_NOT_ACTIVE"):
        detail = text.split(":", 1)[1] if ":" in text else text

        if "CANCEL" in detail:
            return "RFC_CANCELADO"

        if "BAJA" in detail or "DEFINITIV" in detail:
            return "RFC_DADO_DE_BAJA"

        if (
            "INACTIV" in detail
            or "NO ACTIVO" in detail
            or "NO ACTIVA" in detail
        ):
            return "RFC_INACTIVO"

        if "SUSPEND" in detail:
            # LOTES sí permite suspendidos.
            return "SAT_TEMPORAL_ERROR"

    return "SAT_TEMPORAL_ERROR"


def reason_label(code: str | None) -> str:
    return REASON_LABELS.get(
        normalize_status(code),
        "NO FUE POSIBLE VALIDAR",
    )


def terminal_payload(*, code: str, rfc: str, idcif: str) -> dict[str, object]:
    normalized_code = normalize_status(code)
    return {
        "ok": False,
        "valid": False,
        "terminal": normalized_code in TERMINAL_CODES,
        "code": normalized_code,
        "rfc": str(rfc or "").strip().upper(),
        "idcif": str(idcif or "").strip(),
        "reason": reason_label(normalized_code),
    }

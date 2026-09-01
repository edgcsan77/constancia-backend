import os
import time
import threading
from pathlib import Path

import requests


ASPOSE_TOKEN_TIMEOUT = int(os.getenv("ASPOSE_TOKEN_TIMEOUT", "15"))
ASPOSE_CONVERT_TIMEOUT = int(os.getenv("ASPOSE_CONVERT_TIMEOUT", "60"))


# ============================================================
# ASPOSE TOKEN CACHE
# ============================================================

_ASPOSE_TOKEN = None
_ASPOSE_TOKEN_EXPIRES_AT = 0.0
_ASPOSE_TOKEN_LOCK = threading.Lock()


def _aspose_base_url() -> str:
    """
    Puedes setear ASPOSE_BASE_URL:
    - https://api.aspose.cloud
    - https://api.eu.aspose.cloud
    """
    return (
        os.getenv("ASPOSE_BASE_URL")
        or "https://api.aspose.cloud"
    ).rstrip("/")


def _get_aspose_token(
    client_id: str,
    client_secret: str,
    force_refresh: bool = False,
) -> str:
    """
    Obtiene y reutiliza el token OAuth de Aspose.

    Antes:
        Cada PDF pedía un token nuevo.

    Ahora:
        Se obtiene una vez y se reutiliza hasta
        poco antes de su expiración.
    """
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT

    now = time.time()

    if (
        not force_refresh
        and _ASPOSE_TOKEN
        and now < (_ASPOSE_TOKEN_EXPIRES_AT - 60)
    ):
        return _ASPOSE_TOKEN

    with _ASPOSE_TOKEN_LOCK:
        now = time.time()

        # Otro thread pudo renovarlo mientras esperábamos.
        if (
            not force_refresh
            and _ASPOSE_TOKEN
            and now < (_ASPOSE_TOKEN_EXPIRES_AT - 60)
        ):
            return _ASPOSE_TOKEN

        base = _aspose_base_url()
        token_url = f"{base}/connect/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }

        r = requests.post(
            token_url,
            data=data,
            timeout=ASPOSE_TOKEN_TIMEOUT,
        )

        if not r.ok:
            retry_after = (
                r.headers.get("Retry-After")
                or r.headers.get("retry-after")
                or ""
            )

            extra = (
                f" retry_after={retry_after}"
                if retry_after
                else ""
            )

            raise RuntimeError(
                f"Aspose token error {r.status_code}: "
                f"{(r.text or '')[:500]}"
                f"{extra}"
            )

        try:
            j = r.json()
        except Exception as e:
            raise RuntimeError(
                "Aspose token non-json: "
                f"{type(e).__name__}: "
                f"{(r.text or '')[:500]}"
            )

        token = j.get("access_token")

        if not token:
            raise RuntimeError(
                "Aspose token sin access_token: "
                f"{str(j)[:500]}"
            )

        try:
            expires_in = int(
                j.get("expires_in")
                or 3600
            )
        except Exception:
            expires_in = 3600

        _ASPOSE_TOKEN = token
        _ASPOSE_TOKEN_EXPIRES_AT = (
            time.time()
            + max(expires_in, 120)
        )

        print(
            "[ASPOSE TOKEN REFRESHED]",
            {
                "expires_in": expires_in,
            },
            flush=True,
        )

        return _ASPOSE_TOKEN


def _invalidate_aspose_token():
    global _ASPOSE_TOKEN
    global _ASPOSE_TOKEN_EXPIRES_AT

    with _ASPOSE_TOKEN_LOCK:
        _ASPOSE_TOKEN = None
        _ASPOSE_TOKEN_EXPIRES_AT = 0.0


def _docx_to_pdf_aspose_rest(
    docx_path: str,
    pdf_path: str,
) -> str:

    client_id = (
        os.getenv("ASPOSE_CLIENT_ID")
        or ""
    ).strip()

    client_secret = (
        os.getenv("ASPOSE_CLIENT_SECRET")
        or ""
    ).strip()

    if not client_id or not client_secret:
        raise RuntimeError(
            "❌ Faltan variables "
            "ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET"
        )

    base = _aspose_base_url()

    convert_url = (
        f"{base}/v4.0/words/convert?format=pdf"
    )

    with open(docx_path, "rb") as f:
        doc_bytes = f.read()

    if len(doc_bytes) < 50_000:
        raise RuntimeError(
            f"ASPOSE_INPUT_DOCX_TOO_SMALL:"
            f"{len(doc_bytes)}"
        )

    # --------------------------------------------------------
    # Intento normal usando token cacheado.
    # Si el token expiró/revocó y Aspose responde 401,
    # se renueva una sola vez y se reintenta.
    # --------------------------------------------------------

    for auth_attempt in range(2):

        token = _get_aspose_token(
            client_id,
            client_secret,
            force_refresh=(auth_attempt == 1),
        )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream",
        }

        r = requests.put(
            convert_url,
            headers=headers,
            data=doc_bytes,
            timeout=ASPOSE_CONVERT_TIMEOUT,
        )

        if r.status_code == 401 and auth_attempt == 0:
            print(
                "[ASPOSE TOKEN 401 - REFRESH]",
                flush=True,
            )

            _invalidate_aspose_token()
            continue

        if not r.ok:
            raise RuntimeError(
                f"Aspose convert error "
                f"{r.status_code}: "
                f"{(r.text or '')[:800]}"
            )

        if len(r.content or b"") < 10_000:
            raise RuntimeError(
                f"ASPOSE_OUTPUT_PDF_TOO_SMALL:"
                f"{len(r.content or b'')}"
            )

        Path(pdf_path).parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        Path(pdf_path).write_bytes(
            r.content
        )

        return pdf_path

    raise RuntimeError(
        "ASPOSE_AUTH_FAILED_AFTER_REFRESH"
    )


def docx_to_pdf_aspose(
    docx_path: str,
    pdf_path: str,
) -> str:
    """
    WA: REST directo con token cacheado.
    """
    return _docx_to_pdf_aspose_rest(
        docx_path,
        pdf_path,
    )


def docx_to_pdf_aspose_web(
    docx_path: str,
    pdf_path: str,
) -> str:
    """
    WEB: mismo conversor REST directo
    con token cacheado.
    """
    return _docx_to_pdf_aspose_rest(
        docx_path,
        pdf_path,
    )

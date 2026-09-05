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

# ============================================================
# ASPOSE PRIMARY -> GOTENBERG FALLBACK
# ASPOSE_PRIMARY_GOTENBERG_FALLBACK_RENDER_V1
# ============================================================

def _pdf_env_true(
    name: str,
    default: str = "0",
) -> bool:
    return (
        str(os.getenv(name, default) or "")
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )


def _docx_to_pdf_gotenberg_fallback(
    docx_path: str,
    pdf_path: str,
) -> str:
    """Fallback Gotenberg cuando Aspose Cloud falla."""

    if not _pdf_env_true(
        "GOTENBERG_FALLBACK_ENABLED",
        "0",
    ):
        raise RuntimeError("GOTENBERG_FALLBACK_DISABLED")

    gotenberg_url = (
        os.getenv("GOTENBERG_FALLBACK_URL")
        or ""
    ).strip().rstrip("/")

    if not gotenberg_url:
        raise RuntimeError("GOTENBERG_FALLBACK_URL_EMPTY")

    try:
        gotenberg_timeout = int(
            os.getenv(
                "GOTENBERG_FALLBACK_TIMEOUT",
                "25",
            )
        )
    except Exception:
        gotenberg_timeout = 25

    normalize_layout = _pdf_env_true(
        "GOTENBERG_ASPOSE_LAYOUT_NORMALIZE",
        "1",
    )

    working_docx = docx_path
    normalized_docx = None

    try:
        if normalize_layout:
            from gotenberg_aspose import (
                prepare_docx_for_gotenberg,
                normalize_pdf_to_aspose_layout,
            )

            working_docx = prepare_docx_for_gotenberg(
                docx_path
            )
            normalized_docx = working_docx

        endpoint = (
            f"{gotenberg_url}/forms/libreoffice/convert"
        )

        print(
            "[GOTENBERG FALLBACK TRY]",
            {
                "docx": working_docx,
                "pdf": pdf_path,
                "url": gotenberg_url,
            },
            flush=True,
        )

        with open(working_docx, "rb") as f:
            response = requests.post(
                endpoint,
                files={
                    "files": (
                        os.path.basename(working_docx),
                        f,
                        (
                            "application/vnd.openxmlformats-"
                            "officedocument.wordprocessingml.document"
                        ),
                    )
                },
                timeout=(5, gotenberg_timeout),
            )

        if not response.ok:
            raise RuntimeError(
                "GOTENBERG_HTTP_"
                f"{response.status_code}:"
                f"{(response.text or '')[:500]}"
            )

        content = response.content or b""

        if (
            len(content) < 10_000
            or not content.startswith(b"%PDF")
        ):
            raise RuntimeError(
                "GOTENBERG_OUTPUT_INVALID:"
                f"bytes={len(content)}"
            )

        Path(pdf_path).parent.mkdir(
            parents=True,
            exist_ok=True,
        )
        Path(pdf_path).write_bytes(content)

        if normalize_layout:
            normalize_pdf_to_aspose_layout(pdf_path)

        pdf_bytes = Path(pdf_path).read_bytes()

        if (
            len(pdf_bytes) < 10_000
            or not pdf_bytes.startswith(b"%PDF")
        ):
            raise RuntimeError(
                "GOTENBERG_NORMALIZED_PDF_INVALID:"
                f"bytes={len(pdf_bytes)}"
            )

        print(
            "[GOTENBERG FALLBACK OK]",
            {
                "pdf": pdf_path,
                "bytes": len(pdf_bytes),
            },
            flush=True,
        )

        return pdf_path

    finally:
        if (
            normalized_docx
            and normalized_docx != docx_path
        ):
            try:
                os.remove(normalized_docx)
            except Exception:
                pass


def _docx_to_pdf_aspose_with_gotenberg_fallback(
    docx_path: str,
    pdf_path: str,
) -> str:
    """
    Orden definitivo:
      1. Aspose Cloud.
      2. Si Aspose falla -> Gotenberg.
    """

    print(
        "[PDF PRIMARY ASPOSE TRY]",
        docx_path,
        "->",
        pdf_path,
        flush=True,
    )

    try:
        result = _docx_to_pdf_aspose_rest(
            docx_path,
            pdf_path,
        )

        print(
            "[PDF PRIMARY ASPOSE OK]",
            {
                "pdf": pdf_path,
                "bytes": (
                    os.path.getsize(pdf_path)
                    if os.path.exists(pdf_path)
                    else 0
                ),
            },
            flush=True,
        )
        return result

    except Exception as aspose_error:
        print(
            "[PDF PRIMARY ASPOSE FAIL -> GOTENBERG]",
            {
                "error_type": type(aspose_error).__name__,
                "error": str(aspose_error)[:500],
            },
            flush=True,
        )

        if not _pdf_env_true(
            "GOTENBERG_FALLBACK_ENABLED",
            "0",
        ):
            raise

        try:
            return _docx_to_pdf_gotenberg_fallback(
                docx_path,
                pdf_path,
            )
        except Exception as gotenberg_error:
            print(
                "[GOTENBERG FALLBACK FAIL]",
                {
                    "error_type": type(gotenberg_error).__name__,
                    "error": str(gotenberg_error)[:500],
                },
                flush=True,
            )
            raise RuntimeError(
                "ASPOSE_AND_GOTENBERG_FAILED:"
                f"ASPOSE={type(aspose_error).__name__}:"
                f"{str(aspose_error)[:250]} | "
                "GOTENBERG="
                f"{type(gotenberg_error).__name__}:"
                f"{str(gotenberg_error)[:250]}"
            ) from gotenberg_error


def docx_to_pdf_aspose(
    docx_path: str,
    pdf_path: str,
) -> str:
    """WA: Aspose principal, Gotenberg como fallback."""
    return _docx_to_pdf_aspose_with_gotenberg_fallback(
        docx_path,
        pdf_path,
    )


def docx_to_pdf_aspose_web(
    docx_path: str,
    pdf_path: str,
) -> str:
    """WEB/lotes: Aspose principal, Gotenberg como fallback."""
    return _docx_to_pdf_aspose_with_gotenberg_fallback(
        docx_path,
        pdf_path,
    )

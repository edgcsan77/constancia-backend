import os
import time
from pathlib import Path

import requests

# =========================================================
# (WA) tu función actual (déjala igual si ya te funciona)
# =========================================================
from asposewordscloud import WordsApi
from asposewordscloud.models.requests import ConvertDocumentRequest

def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    (WA) Convierte DOCX a PDF usando Aspose Words Cloud SDK.
    NO TOCAR si ya te funciona en WhatsApp.
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # OJO: esto en tu WA funciona con tu imagen docker
    words_api = WordsApi(client_id=client_id, client_secret=client_secret)

    with open(docx_path, "rb") as f:
        req = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(req)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path


# =========================================================
# (WEB) conversión por REST (sin SDK) -> evita RequestId/Modelos
# =========================================================
_ASPOSE_TOKEN_CACHE = {"token": None, "exp": 0}

def _aspose_get_token() -> str:
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    now = int(time.time())
    if _ASPOSE_TOKEN_CACHE["token"] and now < (_ASPOSE_TOKEN_CACHE["exp"] - 30):
        return _ASPOSE_TOKEN_CACHE["token"]

    # Aspose OAuth
    resp = requests.post(
        "https://api.aspose.cloud/connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()
    token = data["access_token"]
    expires_in = int(data.get("expires_in", 3600))

    _ASPOSE_TOKEN_CACHE["token"] = token
    _ASPOSE_TOKEN_CACHE["exp"] = now + expires_in
    return token


def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) DOCX -> PDF por REST (NO SDK).
    - Evita: KeyError('RequestId'), SaveOptionsData missing, etc.
    - Requiere requests (ya lo tienes).
    """
    token = _aspose_get_token()

    # ConvertDocument REST:
    # POST https://api.aspose.cloud/v4.0/words/convert?format=pdf
    url = "https://api.aspose.cloud/v4.0/words/convert?format=pdf"
    headers = {"Authorization": f"Bearer {token}"}

    with open(docx_path, "rb") as f:
        resp = requests.post(url, headers=headers, data=f, timeout=60)

    resp.raise_for_status()
    pdf_bytes = resp.content

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

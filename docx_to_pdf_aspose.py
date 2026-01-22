# docx_to_pdf_aspose.py
import os
from pathlib import Path
import requests

# =========================
# (WA) TU FUNCIÓN ACTUAL
# =========================
from asposewordscloud import WordsApi
from asposewordscloud.models.requests import ConvertDocumentRequest

def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    (WA) Convierte DOCX a PDF usando SDK.
    NO TOCAR si ya te funciona en WhatsApp.
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # Si WA ya funciona así, déjalo.
    words_api = WordsApi(client_id=client_id, client_secret=client_secret)

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path


# =========================
# (WEB) REST DIRECTO (PUT)
# =========================
def _aspose_get_token(client_id: str, client_secret: str) -> str:
    # Aspose: connect/token (client_credentials) :contentReference[oaicite:1]{index=1}
    url = "https://api.aspose.cloud/connect/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    r = requests.post(url, data=data, headers={"Accept": "application/json"}, timeout=30)
    r.raise_for_status()
    j = r.json()
    token = j.get("access_token") or j.get("token")
    if not token:
        raise RuntimeError(f"❌ Token inválido: {j}")
    return token


def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) Convierte DOCX->PDF por REST (sin SDK).
    - Evita errores de compatibilidad (WordsApi init, SaveOptionsData, RequestId).
    - Usa el método correcto: PUT /v4.0/words/convert?format=pdf :contentReference[oaicite:2]{index=2}
    """
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    token = _aspose_get_token(client_id, client_secret)

    convert_url = "https://api.aspose.cloud/v4.0/words/convert"
    params = {"format": "pdf"}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "multipart/form-data",
    }

    with open(docx_path, "rb") as f:
        files = {"document": f}  # campo 'document' :contentReference[oaicite:3]{index=3}
        r = requests.put(convert_url, params=params, headers=headers, files=files, timeout=90)
        r.raise_for_status()
        pdf_bytes = r.content

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

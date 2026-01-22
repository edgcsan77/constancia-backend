import os
from pathlib import Path

from asposewordscloud import WordsApi, ApiClient, Configuration
from asposewordscloud.models.requests import ConvertDocumentRequest


def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    (WA) Convierte DOCX a PDF usando Aspose Words Cloud.
    NO TOCAR si ya te funciona en WhatsApp.
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # ⚠️ Se queda tal cual (solo si realmente te funciona en WA)
    words_api = WordsApi(
        client_id=client_id,
        client_secret=client_secret
    )

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    with open(pdf_path, "wb") as out:
        out.write(pdf_bytes)

    return pdf_path

def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) Compatible con ambas versiones del SDK:
    - WordsApi(client_id, client_secret)  (firma vieja)
    - WordsApi(ApiClient(Configuration))  (firma nueva)
    """
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # 1) Intenta firma vieja: WordsApi(client_id, client_secret) (posicional)
    api = None
    try:
        api = WordsApi(client_id, client_secret)
    except TypeError:
        api = None

    # 2) Si no jaló, intenta firma nueva: WordsApi(ApiClient(cfg))
    if api is None:
        cfg = Configuration()
        cfg.client_id = client_id
        cfg.client_secret = client_secret
        api = WordsApi(ApiClient(cfg))

    with open(docx_path, "rb") as f:
        req = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = api.convert_document(req)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

import os
from pathlib import Path

from asposewordscloud import WordsApi
from asposewordscloud.models.requests import ConvertDocumentRequest

def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    Convierte DOCX a PDF usando Aspose Words Cloud
    Mantiene formato casi idéntico a Word original
    """

    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    words_api = WordsApi(
        client_id=client_id,
        client_secret=client_secret
    )

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(
            document=f,
            format="pdf"
        )
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)

    with open(pdf_path, "wb") as out:
        out.write(pdf_bytes)

    return pdf_path

from asposewordscloud import Configuration, ApiClient  # <-- nuevo import

def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    Solo para la WEB.
    Usa Configuration+ApiClient (más estable) y ayuda a evitar KeyError('RequestId').
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    cfg = Configuration(client_id=client_id, client_secret=client_secret)
    api = WordsApi(ApiClient(cfg))

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

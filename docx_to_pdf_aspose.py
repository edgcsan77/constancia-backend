import os
from pathlib import Path

from asposewordscloud import WordsApi
from asposewordscloud.models.requests import ConvertDocumentRequest, SaveAsOnlineRequest

# ✅ IMPORT CORRECTO para SaveOptionsData (según versión)
try:
    from asposewordscloud.models import SaveOptionsData
except Exception:
    from asposewordscloud.models.save_options_data import SaveOptionsData


# =========================================================
# (WA) NO TOCAR: deja tu implementación igual si ya funciona
# =========================================================
def docx_to_pdf_aspose(docx_path: str, pdf_path: str) -> str:
    """
    (WA) Convierte DOCX a PDF usando Aspose Words Cloud.
    NO TOCAR si ya te funciona en WhatsApp.
    """
    client_id = os.getenv("ASPOSE_CLIENT_ID")
    client_secret = os.getenv("ASPOSE_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    words_api = WordsApi(client_id=client_id, client_secret=client_secret)

    with open(docx_path, "rb") as f:
        request = ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path


def _words_api_from_env() -> WordsApi:
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # keywords -> si no, posicional
    try:
        return WordsApi(client_id=client_id, client_secret=client_secret)
    except TypeError:
        return WordsApi(client_id, client_secret)


def _bytes_from_aspose_result(result):
    """
    Normaliza lo que regresa el SDK:
    - bytes
    - objeto con .read()
    - dict con "document"/"response" (varía por versión)
    """
    if isinstance(result, (bytes, bytearray)):
        return bytes(result)

    if hasattr(result, "read"):
        return result.read()

    # Algunas versiones devuelven un dict-like
    if isinstance(result, dict):
        for k in ("document", "response", "data", "body"):
            v = result.get(k)
            if isinstance(v, (bytes, bytearray)):
                return bytes(v)
            if hasattr(v, "read"):
                return v.read()

    raise RuntimeError(f"No pude extraer bytes del resultado Aspose: {type(result)}")


def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) Convierte DOCX->PDF tolerando:
    - Diferencias de init del SDK
    - Bug KeyError('RequestId') en convert_document
    Fallback: SaveAsOnline
    """
    api = _words_api_from_env()
    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)

    # ===== Intento 1: convert_document =====
    try:
        with open(docx_path, "rb") as f:
            request = ConvertDocumentRequest(document=f, format="pdf")
            pdf_bytes = api.convert_document(request)

        Path(pdf_path).write_bytes(pdf_bytes)
        return pdf_path

    except KeyError as e:
        # famoso KeyError('RequestId')
        if str(e) != "'RequestId'":
            raise

    # ===== Fallback: save_as_online =====
    with open(docx_path, "rb") as f:
        save_opts = SaveOptionsData(save_format="pdf", file_name="out.pdf")

        # en algunas versiones el parámetro se llama save_options_data
        request = SaveAsOnlineRequest(document=f, save_options_data=save_opts)

        result = api.save_as_online(request)

    pdf_bytes = _bytes_from_aspose_result(result)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

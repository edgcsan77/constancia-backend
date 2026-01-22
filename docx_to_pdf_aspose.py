import os
from pathlib import Path

from asposewordscloud import WordsApi
import asposewordscloud
import asposewordscloud.models.requests as reqs

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
        request = reqs.ConvertDocumentRequest(document=f, format="pdf")
        pdf_bytes = words_api.convert_document(request)

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path


# =========================================================
# (WEB) Robusto: init compatible + fallback para KeyError(RequestId)
# =========================================================
def _words_api_from_env() -> WordsApi:
    client_id = (os.getenv("ASPOSE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("ASPOSE_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise RuntimeError("❌ Faltan variables ASPOSE_CLIENT_ID / ASPOSE_CLIENT_SECRET")

    # 1) intenta keywords (algunas versiones lo soportan)
    try:
        return WordsApi(client_id=client_id, client_secret=client_secret)
    except TypeError:
        pass

    # 2) intenta posicional (varios ejemplos oficiales usan esto)
    return WordsApi(client_id, client_secret)


def docx_to_pdf_aspose_web(docx_path: str, pdf_path: str) -> str:
    """
    (WEB) Convierte DOCX->PDF tolerando diferencias del SDK y el bug KeyError('RequestId').
    - Intento 1: convert_document
    - Fallback: save_as_online (suele evitar el RequestId)
    """
    api = _words_api_from_env()

    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)

    # -------- Intento 1: convert_document --------
    try:
        with open(docx_path, "rb") as f:
            request = reqs.ConvertDocumentRequest(document=f, format="pdf")
            pdf_bytes = api.convert_document(request)

        Path(pdf_path).write_bytes(pdf_bytes)
        return pdf_path

    except KeyError as e:
        # el famoso KeyError('RequestId')
        if str(e) != "'RequestId'":
            raise

    # -------- Fallback: save_as_online --------
    # Convierte "online" (sin storage) y regresa el PDF como bytes/stream
    with open(docx_path, "rb") as f:
        document = f
        save_options = asposewordscloud.SaveOptionsData(save_format="pdf", file_name="out.pdf")
        request = reqs.SaveAsOnlineRequest(document=document, save_options_data=save_options)
        result = api.save_as_online(request)

    # result suele ser bytes o un objeto con read(); lo normalizamos
    pdf_bytes = result if isinstance(result, (bytes, bytearray)) else result.read()

    Path(pdf_path).write_bytes(pdf_bytes)
    return pdf_path

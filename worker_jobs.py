import os
import traceback
import base64
import requests
from urllib.parse import urlsplit, urlunsplit

from rfc import procesar_solicitud_interna_para_pdf

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()

def store_download_via_web(filename: str, file_bytes: bytes, ttl_sec: int = 86400) -> str:
    url = f"{BOT_INTERNAL_URL.rsplit('/internal/generate-pdf', 1)[0]}/internal/store-download"
    headers = {
        "Authorization": f"Bearer {BOT_INTERNAL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "filename": filename,
        "content_b64": base64.b64encode(file_bytes).decode("utf-8"),
        "ttl_sec": ttl_sec,
    }
    r = requests.post(url, json=payload, headers=headers, timeout=180)
    print("store_download_via_web:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok") or not data.get("url"):
        raise RuntimeError("STORE_DOWNLOAD_FAILED")
    return data["url"]

def evolution_headers():
    return {
        "apikey": EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


def rewrite_public_url(url: str) -> str:
    if not url or not PUBLIC_BASE_URL:
        return url
    parts = urlsplit(url)
    base = urlsplit(PUBLIC_BASE_URL)
    return urlunsplit((base.scheme, base.netloc, parts.path, parts.query, parts.fragment))


def evolution_send_text_to_group(group_jid: str, text: str):
    url = f"{EVOLUTION_BASE_URL}/message/sendText/{EVOLUTION_INSTANCE}"
    payload = {
        "number": group_jid,
        "text": text
    }
    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=60)
    print("worker sendText:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()


def evolution_send_media_to_group(group_jid: str, media_url: str, file_name: str):
    url = f"{EVOLUTION_BASE_URL}/message/sendMedia/{EVOLUTION_INSTANCE}"
    payload = {
        "number": group_jid,
        "mediatype": "document",
        "media": media_url,
        "fileName": file_name,
    }
    r = requests.post(url, json=payload, headers=evolution_headers(), timeout=240)
    print("worker sendMedia payload:", payload, flush=True)
    print("worker sendMedia resp:", r.status_code, r.text, flush=True)
    r.raise_for_status()
    return r.json()


def process_group_request_job(job_data: dict):
    requester_number = job_data["requester_number"]
    requester_name = job_data["requester_name"]
    requester_label = job_data["requester_label"]
    group_jid = job_data["group_jid"]
    original_text = job_data["original_text"]
    query = job_data["query"]

    try:
        result = procesar_solicitud_interna_para_pdf(
            from_wa_id=requester_number,
            text_body=query,
            original_text=original_text,
            source="GROUP_BRIDGE",
            requester_name=requester_name,
            group_jid=group_jid,
        )

        mode = (result.get("mode") or "single").strip().lower()

        if mode == "batch":
            zip_url = (result.get("zip_url") or "").strip()
            file_name = (result.get("filename") or "constancias_lote.zip").strip()
            ok_count = result.get("ok_count", 0)
            fail_count = result.get("fail_count", 0)

            if not zip_url:
                evolution_send_text_to_group(
                    group_jid,
                    f"❌ {requester_label} no se obtuvo enlace del lote."
                )
                return

            zip_url = rewrite_public_url(zip_url)

            try:
                evolution_send_media_to_group(
                    group_jid=group_jid,
                    media_url=zip_url,
                    file_name=file_name,
                )
                evolution_send_text_to_group(
                    group_jid,
                    f"📦 Lote procesado para {requester_label}. Correctos: {ok_count}. Fallidos: {fail_count}."
                )
            except Exception as media_err:
                print("group batch media send fail:", repr(media_err), flush=True)
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el lote se generó, pero no pude adjuntarlo.\n{zip_url}"
                )
            return

        file_name = (result.get("filename") or "documento.pdf").strip()
        pdf_bytes = result.get("pdf_bytes")
        
        if not pdf_bytes:
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} no se obtuvo el PDF."
            )
            return
        
        pdf_url = store_download_via_web(file_name, pdf_bytes, ttl_sec=86400)
        pdf_url = rewrite_public_url(pdf_url)
        
        try:
            evolution_send_media_to_group(
                group_jid=group_jid,
                media_url=pdf_url,
                file_name=file_name,
            )
        except Exception as media_err:
            print("group media send fail:", repr(media_err), flush=True)
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} el documento se generó, pero no pude adjuntarlo.\n{pdf_url}"
            )

        if not pdf_url:
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} no se obtuvo enlace del PDF."
            )
            return

        pdf_url = rewrite_public_url(pdf_url)

        try:
            evolution_send_media_to_group(
                group_jid=group_jid,
                media_url=pdf_url,
                file_name=file_name,
            )
        except Exception as media_err:
            print("group media send fail:", repr(media_err), flush=True)
            evolution_send_text_to_group(
                group_jid,
                f"⚠️ {requester_label} el documento se generó, pero no pude adjuntarlo.\n{pdf_url}"
            )

    except Exception as e:
        print("process_group_request_job error:", repr(e), flush=True)
        traceback.print_exc()
        try:
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} ocurrió un error procesando la solicitud."
            )
        except Exception:
            pass

import os
import traceback
import requests

EVOLUTION_BASE_URL = os.getenv("EVOLUTION_BASE_URL", "").rstrip("/")
EVOLUTION_API_KEY = os.getenv("EVOLUTION_API_KEY", "").strip()
EVOLUTION_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "").strip()

BOT_INTERNAL_URL = os.getenv("BOT_INTERNAL_URL", "").strip()
BOT_INTERNAL_TOKEN = os.getenv("BOT_INTERNAL_TOKEN", "").strip()


def evolution_headers():
    return {
        "apikey": EVOLUTION_API_KEY,
        "Content-Type": "application/json",
    }


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


def call_bot_internal(requester_number: str, requester_name: str, group_jid: str, original_text: str, query: str):
    headers = {
        "Authorization": f"Bearer {BOT_INTERNAL_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "requester_number": requester_number,
        "requester_name": requester_name,
        "group_jid": group_jid,
        "original_text": original_text,
        "query": query,
    }
    r = requests.post(BOT_INTERNAL_URL, json=payload, headers=headers, timeout=420)
    print("worker call_bot_internal status:", r.status_code, flush=True)
    print("worker call_bot_internal resp:", r.text, flush=True)
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
        result = call_bot_internal(
            requester_number=requester_number,
            requester_name=requester_name,
            group_jid=group_jid,
            original_text=original_text,
            query=query,
        )

        if not result.get("ok"):
            err = result.get("error") or "No fue posible generar el documento."
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} {err}"
            )
            return

        mode = (result.get("mode") or "single").strip().lower()

        if mode == "batch_zip":
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

            try:
                evolution_send_media_to_group(
                    group_jid=group_jid,
                    media_url=zip_url,
                    file_name=file_name,
                )
            except Exception as media_err:
                print("group batch zip media send fail:", repr(media_err), flush=True)
                evolution_send_text_to_group(
                    group_jid,
                    f"⚠️ {requester_label} el lote se generó, pero no pude adjuntarlo.\n{zip_url}"
                )
            return

        if mode == "batch_multi":
            items = result.get("items") or []
            ok_count = result.get("ok_count", 0)
            fail_count = result.get("fail_count", 0)

            for item in items:
                pdf_url = (item.get("pdf_url") or "").strip()
                file_name = (item.get("filename") or "documento.pdf").strip()
                err = (item.get("error") or "").strip()
                rfc = (item.get("rfc") or "").strip()
                idcif = (item.get("idcif") or "").strip()

                if pdf_url:
                    try:
                        evolution_send_media_to_group(
                            group_jid=group_jid,
                            media_url=pdf_url,
                            file_name=file_name,
                        )
                    except Exception as media_err:
                        print("group batch multi media send fail:", repr(media_err), flush=True)
                        evolution_send_text_to_group(
                            group_jid,
                            f"⚠️ {requester_label} no pude adjuntar {file_name}.\n{pdf_url}"
                        )
                else:
                    evolution_send_text_to_group(
                        group_jid,
                        f"❌ {requester_label} fallo {rfc} {idcif}: {err or 'error desconocido'}"
                    )

            return

        pdf_url = (result.get("pdf_url") or "").strip()
        file_name = (result.get("filename") or "documento.pdf").strip()

        if not pdf_url:
            evolution_send_text_to_group(
                group_jid,
                f"❌ {requester_label} no se obtuvo enlace del PDF."
            )
            return

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

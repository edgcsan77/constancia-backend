# github_personas.py
import base64
import json
import os
import time
import requests

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "edgcsan77")
GITHUB_REPO = os.getenv("GITHUB_REPO", "validacion-sat")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")

PERSONAS_PATH = os.getenv("PERSONAS_PATH", "public/data/personas.json")


def _gh_headers():
    if not GITHUB_TOKEN:
        raise RuntimeError("❌ Falta GITHUB_TOKEN en Render")
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def github_get_personas():
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{PERSONAS_PATH}"
    r = requests.get(url, headers=_gh_headers(), params={"ref": GITHUB_BRANCH}, timeout=30)

    if r.status_code == 404:
        # Si no existe, inicializamos vacío (y sha None)
        return {}, None

    if r.status_code != 200:
        raise RuntimeError(f"GitHub GET error {r.status_code}: {r.text[:500]}")

    payload = r.json()
    content_b64 = payload.get("content") or ""
    if not content_b64:
        return {}, payload.get("sha")

    content = base64.b64decode(content_b64).decode("utf-8", errors="replace")
    try:
        data = json.loads(content)
        if not isinstance(data, dict):
            # esperamos dict { "D3": {...} }
            data = {}
    except Exception:
        data = {}

    return data, payload.get("sha")


def github_put_personas(db: dict, sha: str | None, message: str):
    url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{PERSONAS_PATH}"

    body = {
        "message": message,
        "content": base64.b64encode(
            json.dumps(db, ensure_ascii=False, indent=2).encode("utf-8")
        ).decode("utf-8"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(url, headers=_gh_headers(), json=body, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"GitHub PUT error {r.status_code}: {r.text[:800]}")
    return r.json()


def github_upsert_persona(persona: dict, max_retries: int = 3):
    """
    UPSERT por key persona["D3"].
    Reintenta si hay conflicto de SHA (409) por concurrencia.
    """
    if not isinstance(persona, dict):
        raise ValueError("persona debe ser dict")
    if not persona.get("D3"):
        raise ValueError("persona.D3 es obligatorio")

    key = str(persona["D3"]).strip()

    last_err = None
    for attempt in range(1, max_retries + 1):
        db, sha = github_get_personas()
        db[key] = persona

        try:
            return github_put_personas(
                db=db,
                sha=sha,
                message=f"Update personas.json ({key})",
            )
        except RuntimeError as e:
            msg = str(e)
            last_err = e

            # Conflicto típico: 409 si cambió el sha entre GET y PUT
            if " 409" in msg or "409:" in msg:
                time.sleep(0.4 * attempt)
                continue
            raise

    raise last_err or RuntimeError("No se pudo actualizar personas.json")

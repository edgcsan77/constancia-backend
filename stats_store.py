# stats_store.py
# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

MAX_RFC_HISTORY = 200          # historial de intentos por user (attempts)
MAX_OK_RFC_PER_USER = 80       # RFCs OK por user (solo exitos)
MAX_OK_RFC_GLOBAL = 100        # RFCs OK global (dashboard)

def _now_iso():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).isoformat()
    except Exception:
        return datetime.utcnow().isoformat()

def log_attempt(state: dict, user_key: str, rfc: str | None, ok: bool, code: str, meta: dict | None = None):
    """
    Guarda intentos (OK y FAIL) por usuario (útil para debug/admin).
    code ejemplos:
      OK, SIN_DATOS_SAT, SAT_ERROR, PDF_CONVERT_FAIL, WA_SEND_FAIL, PARSE_FAIL, DAILY_LIMIT
    """
    user_key = user_key or "UNKNOWN"
    state.setdefault("attempts", {})
    state["attempts"].setdefault(user_key, [])
    entry = {
        "ts": _now_iso(),
        "rfc": (rfc or "").upper(),
        "ok": bool(ok),
        "code": code,
        "meta": meta or {},
    }
    state["attempts"][user_key].append(entry)
    if len(state["attempts"][user_key]) > MAX_RFC_HISTORY:
        state["attempts"][user_key] = state["attempts"][user_key][-MAX_RFC_HISTORY:]

def _default_state():
    return {
        "request_total": 0,
        "success_total": 0,
        "por_dia": {},        # "YYYY-MM-DD": {"requests": n, "success": n}
        "por_usuario": {},    # "user": {"hoy": "YYYY-MM-DD", "count": n, "success": n, "last_success": [...]}
        "last_success": [],   # RFCs ok global (máx 100)
        "attempts": {},       # intentos por usuario (OK+FAIL)
        "updated_at": _now_iso(),
    }

def _safe_read(path: str):
    try:
        if not os.path.exists(path):
            return _default_state()
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
            # garantizar llaves mínimas
            base = _default_state()
            base.update(data)
            base.setdefault("por_dia", {})
            base.setdefault("por_usuario", {})
            base.setdefault("last_success", [])
            base.setdefault("attempts", {})
            return base
    except Exception:
        return _default_state()

def _safe_write(path: str, data: dict):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def get_and_update(path: str, fn):
    """
    Lee estado -> fn(state) lo modifica -> guarda
    """
    state = _safe_read(path)
    try:
        fn(state)
    finally:
        state["updated_at"] = _now_iso()
        _safe_write(path, state)
    return state

def get_state(path: str):
    return _safe_read(path)

# ---------- helpers usados por api.py ----------

def inc_request(state: dict, day: str = None):
    if not day:
        day = _today_str()
    state["request_total"] = int(state.get("request_total", 0)) + 1
    por_dia = state.setdefault("por_dia", {})
    por_dia.setdefault(day, {"requests": 0, "success": 0})
    por_dia[day]["requests"] = int(por_dia[day].get("requests", 0)) + 1

def inc_user_request(state: dict, user: str, day: str = None):
    if not day:
        day = _today_str()
    por_usuario = state.setdefault("por_usuario", {})
    info = por_usuario.get(user) or {"hoy": day, "count": 0, "success": 0, "last_success": []}

    # reset diario
    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0
        # OJO: NO reseteamos last_success (RFC OK históricos)
        # si quieres que sea solo "hoy", entonces sí resetea aquí.

    info["count"] = int(info.get("count", 0)) + 1
    por_usuario[user] = info

def inc_success(state: dict, user: str, rfc: str, day: str = None):
    """
    Marca un éxito:
    - suma success_total
    - suma success por día
    - suma success por usuario
    - guarda RFC OK global (last_success)
    - guarda RFC OK por usuario (por_usuario[user]["last_success"])
    """
    if not day:
        day = _today_str()

    rfc = (str(rfc) if rfc else "").upper().strip()

    state["success_total"] = int(state.get("success_total", 0)) + 1

    por_dia = state.setdefault("por_dia", {})
    por_dia.setdefault(day, {"requests": 0, "success": 0})
    por_dia[day]["success"] = int(por_dia[day].get("success", 0)) + 1

    por_usuario = state.setdefault("por_usuario", {})
    info = por_usuario.get(user) or {"hoy": day, "count": 0, "success": 0, "last_success": []}

    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0
        info["success"] = 0

    info["success"] = int(info.get("success", 0)) + 1

    # ✅ RFC OK por usuario
    if rfc:
        lst_u = info.setdefault("last_success", [])
        lst_u.append(rfc)
        if len(lst_u) > MAX_OK_RFC_PER_USER:
            del lst_u[:-MAX_OK_RFC_PER_USER]

    por_usuario[user] = info

    # ✅ RFC OK global (dashboard)
    if rfc:
        last = state.setdefault("last_success", [])
        last.append(rfc)
        if len(last) > MAX_OK_RFC_GLOBAL:
            del last[:-MAX_OK_RFC_GLOBAL]

def _today_str():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()

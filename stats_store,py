# stats_store.py
# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

def _now_iso():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).isoformat()
    except Exception:
        return datetime.utcnow().isoformat()

def _default_state():
    return {
        "request_total": 0,
        "success_total": 0,
        "por_dia": {},        # "YYYY-MM-DD": {"requests": n, "success": n}
        "por_usuario": {},    # "user": {"hoy": "YYYY-MM-DD", "count": n, "success": n}
        "last_success": [],   # lista RFCs ok (máx 100)
        "updated_at": _now_iso(),
    }

def _safe_read(path: str):
    try:
        if not os.path.exists(path):
            return _default_state()
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return _default_state()

def _safe_write(path: str, data: dict):
    os.makedirs(os.path.dirname(path), exist_ok=True)
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

# ---------- helpers usados por rfc.py ----------

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
    info = por_usuario.get(user) or {"hoy": day, "count": 0, "success": 0}

    # reset diario
    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0

    info["count"] = int(info.get("count", 0)) + 1
    por_usuario[user] = info

def inc_success(state: dict, user: str, rfc: str, day: str = None):
    if not day:
        day = _today_str()
    state["success_total"] = int(state.get("success_total", 0)) + 1

    por_dia = state.setdefault("por_dia", {})
    por_dia.setdefault(day, {"requests": 0, "success": 0})
    por_dia[day]["success"] = int(por_dia[day].get("success", 0)) + 1

    por_usuario = state.setdefault("por_usuario", {})
    info = por_usuario.get(user) or {"hoy": day, "count": 0, "success": 0}
    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0
        info["success"] = 0
    info["success"] = int(info.get("success", 0)) + 1
    por_usuario[user] = info

    last = state.setdefault("last_success", [])
    if rfc:
        last.append(str(rfc))
        if len(last) > 100:
            del last[:-100]

def _today_str():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()

# stats_store.py
# -*- coding: utf-8 -*-
import os, json
from datetime import datetime
from zoneinfo import ZoneInfo

MAX_RFC_HISTORY = 200
MAX_ATTEMPTS_PER_USER = 300

def _now_iso():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).isoformat()
    except Exception:
        return datetime.utcnow().isoformat()

def _today_str():
    try:
        return datetime.now(ZoneInfo("America/Mexico_City")).date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()

def _default_state():
    return {
        "request_total": 0,
        "success_total": 0,
        "por_dia": {},        # "YYYY-MM-DD": {"requests": n, "success": n}
        "por_usuario": {},    # "user": {"hoy": "YYYY-MM-DD", "count": n, "success": n, "rfcs_ok": [...]}
        "attempts": {},       # "user": [{"ts","rfc","ok","code","meta","is_test"}]
        "last_success": [],   # global últimos OK (máx 100)
        "updated_at": _now_iso(),
        "billing": {
          "price_mxn": 0,
          "total_billed": 0,
          "total_revenue_mxn": 0,
          "by_user": {}  # "user": {"billed": n, "revenue_mxn": n, "rfcs": [..], "last": "..."}
        },
        "rfc_ok_index": {},  # "RFC": {"user": "...", "ts": "..."}  (dedupe global)
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
    state = _safe_read(path)
    try:
        fn(state)
    finally:
        state["updated_at"] = _now_iso()
        _safe_write(path, state)
    return state

def get_state(path: str):
    return _safe_read(path)

def inc_request(state: dict, day: str = None):
    day = day or _today_str()
    state["request_total"] = int(state.get("request_total", 0)) + 1
    por_dia = state.setdefault("por_dia", {})
    por_dia.setdefault(day, {"requests": 0, "success": 0})
    por_dia[day]["requests"] = int(por_dia[day].get("requests", 0)) + 1

def inc_user_request(state: dict, user: str, day: str = None):
    day = day or _today_str()
    pu = state.setdefault("por_usuario", {})
    info = pu.get(user) or {"hoy": day, "count": 0, "success": 0, "rfcs_ok": []}

    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0
        info["success"] = 0  # si quieres que success sea diario también

    info["count"] = int(info.get("count", 0)) + 1
    pu[user] = info

def inc_success(state: dict, user: str, rfc: str, day: str = None):
    day = day or _today_str()
    rfc = (rfc or "").upper().strip()

    state["success_total"] = int(state.get("success_total", 0)) + 1

    por_dia = state.setdefault("por_dia", {})
    por_dia.setdefault(day, {"requests": 0, "success": 0})
    por_dia[day]["success"] = int(por_dia[day].get("success", 0)) + 1

    pu = state.setdefault("por_usuario", {})
    info = pu.get(user) or {"hoy": day, "count": 0, "success": 0, "rfcs_ok": []}
    if info.get("hoy") != day:
        info["hoy"] = day
        info["count"] = 0
        info["success"] = 0
        info["rfcs_ok"] = []

    info["success"] = int(info.get("success", 0)) + 1

    # ✅ guardar RFC OK por usuario
    rfcs_ok = info.setdefault("rfcs_ok", [])
    if rfc:
        rfcs_ok.append(rfc)
        if len(rfcs_ok) > MAX_RFC_HISTORY:
            del rfcs_ok[:-MAX_RFC_HISTORY]

    pu[user] = info

    # ✅ global últimos OK
    last = state.setdefault("last_success", [])
    if rfc:
        last.append(rfc)
        if len(last) > 100:
            del last[:-100]

def log_attempt(state: dict, user_key: str, rfc: str | None, ok: bool, code: str, meta: dict | None = None, is_test: bool = False):
    user_key = user_key or "UNKNOWN"
    state.setdefault("attempts", {})
    state["attempts"].setdefault(user_key, [])

    entry = {
        "ts": _now_iso(),
        "rfc": (rfc or "").upper(),
        "ok": bool(ok),
        "code": code,
        "meta": meta or {},
        "is_test": bool(is_test),
    }
    state["attempts"][user_key].append(entry)
    if len(state["attempts"][user_key]) > MAX_ATTEMPTS_PER_USER:
        state["attempts"][user_key] = state["attempts"][user_key][-MAX_ATTEMPTS_PER_USER:]
        
def set_price(state: dict, price_mxn: int):
    billing = state.setdefault("billing", {})
    billing["price_mxn"] = int(price_mxn or 0)

def is_rfc_already_billed(state: dict, rfc: str) -> bool:
    rfc = (rfc or "").upper().strip()
    idx = state.get("rfc_ok_index") or {}
    return bool(rfc and rfc in idx)

def bill_success_if_new(state: dict, user: str, rfc: str, is_test: bool = False) -> dict:
    """
    Si el RFC es NUEVO globalmente => cobra (billed++)
    Si ya existía => DUPLICADO (no cobra)

    Regresa dict:
    { "billed": True/False, "reason": "NEW_OK"|"DUPLICATE"|"TEST"|"EMPTY_RFC" }
    """
    user = user or "UNKNOWN"
    rfc = (rfc or "").upper().strip()
    if not rfc:
        return {"billed": False, "reason": "EMPTY_RFC"}
    if is_test:
        return {"billed": False, "reason": "TEST"}

    state.setdefault("rfc_ok_index", {})
    idx = state["rfc_ok_index"]

    if rfc in idx:
        return {"billed": False, "reason": "DUPLICATE"}

    # registrar RFC como cobrado (dedupe global)
    idx[rfc] = {"user": user, "ts": _now_iso()}

    billing = state.setdefault("billing", {})
    by_user = billing.setdefault("by_user", {})

    price = int(billing.get("price_mxn") or 0)

    # totales globales
    billing["total_billed"] = int(billing.get("total_billed") or 0) + 1
    billing["total_revenue_mxn"] = int(billing.get("total_revenue_mxn") or 0) + price

    # por usuario
    u = by_user.get(user) or {"billed": 0, "revenue_mxn": 0, "rfcs": [], "last": ""}
    u["billed"] = int(u.get("billed") or 0) + 1
    u["revenue_mxn"] = int(u.get("revenue_mxn") or 0) + price
    u["last"] = _now_iso()

    rfcs = u.get("rfcs") or []
    rfcs.append(rfc)
    # guarda últimos 200 cobrados por usuario
    if len(rfcs) > 200:
        rfcs = rfcs[-200:]
    u["rfcs"] = rfcs

    by_user[user] = u

    return {"billed": True, "reason": "NEW_OK"}

def is_blocked(state: dict, user_key: str) -> bool:
    user_key = (user_key or "").strip()
    blocked = state.get("blocked_users") or {}
    return bool(user_key and blocked.get(user_key))

def block_user(state: dict, user_key: str, reason: str = ""):
    user_key = (user_key or "").strip()
    if not user_key:
        return
    state.setdefault("blocked_users", {})
    state["blocked_users"][user_key] = {
        "ts": _now_iso(),
        "reason": reason or "blocked"
    }

def unblock_user(state: dict, user_key: str):
    user_key = (user_key or "").strip()
    if not user_key:
        return
    (state.get("blocked_users") or {}).pop(user_key, None)

def unbill_rfc(state: dict, rfc: str) -> dict:
    """
    Elimina RFC del índice global y revierte billing si estaba cobrado.
    También lo quita de listas visibles (last_success, rfcs_ok, billing rfcs).
    """
    rfc = (rfc or "").upper().strip()
    if not rfc:
        return {"ok": False, "reason": "EMPTY_RFC"}

    idx = state.get("rfc_ok_index") or {}
    if rfc not in idx:
        # aun así limpiamos apariciones en listas
        _remove_rfc_from_lists(state, rfc)
        return {"ok": True, "removed": False, "reason": "NOT_FOUND"}

    owner = (idx.get(rfc) or {}).get("user") or "UNKNOWN"

    # 1) borrar del índice
    idx.pop(rfc, None)
    state["rfc_ok_index"] = idx

    # 2) revertir billing (si tu sistema considera que todo rfc_ok_index es cobrado)
    billing = state.setdefault("billing", {})
    price = int(billing.get("price_mxn") or 0)

    billing["total_billed"] = max(0, int(billing.get("total_billed") or 0) - 1)
    billing["total_revenue_mxn"] = max(0, int(billing.get("total_revenue_mxn") or 0) - price)

    by_user = billing.setdefault("by_user", {})
    u = by_user.get(owner) or {"billed": 0, "revenue_mxn": 0, "rfcs": [], "last": ""}

    u["billed"] = max(0, int(u.get("billed") or 0) - 1)
    u["revenue_mxn"] = max(0, int(u.get("revenue_mxn") or 0) - price)

    rfcs = list(u.get("rfcs") or [])
    rfcs = [x for x in rfcs if (x or "").upper().strip() != rfc]
    u["rfcs"] = rfcs
    u["last"] = _now_iso()
    by_user[owner] = u

    # 3) limpiar listas “visibles”
    _remove_rfc_from_lists(state, rfc)

    return {"ok": True, "removed": True, "owner": owner, "price": price}

def _remove_rfc_from_lists(state: dict, rfc: str):
    rfc = (rfc or "").upper().strip()
    if not rfc:
        return

    # global last_success
    last = list(state.get("last_success") or [])
    state["last_success"] = [x for x in last if (x or "").upper().strip() != rfc]

    # por_usuario rfcs_ok
    pu = state.get("por_usuario") or {}
    for user, info in pu.items():
        if not isinstance(info, dict):
            continue
        rfcs_ok = list(info.get("rfcs_ok") or [])
        info["rfcs_ok"] = [x for x in rfcs_ok if (x or "").upper().strip() != rfc]
        pu[user] = info
    state["por_usuario"] = pu

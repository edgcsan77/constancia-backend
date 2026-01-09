# stats_store.py
# -*- coding: utf-8 -*-
import os, json
from datetime import datetime
from zoneinfo import ZoneInfo

MAX_RFC_HISTORY = 200
MAX_ATTEMPTS_PER_USER = 300

# Tipos de entrada (para billing)
INPUT_TYPES = ("CURP", "RFC_IDCIF", "QR")

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
        "attempts": {},       # "user": [{"ts","key","ok","code","meta","is_test"}]
        "last_success": [],   # global últimos OK (máx 100)
        "updated_at": _now_iso(),

        # ✅ billing + pricing
        "billing": {
            "total_billed": 0,
            "total_revenue_mxn": 0,
            "by_user": {},      # "user": {"billed": n, "revenue_mxn": n, "by_type": {...}, "keys": [...], "last": "..."}
            "by_type": {        # global por tipo
                "CURP": {"billed": 0, "revenue_mxn": 0},
                "RFC_IDCIF": {"billed": 0, "revenue_mxn": 0},
                "QR": {"billed": 0, "revenue_mxn": 0},
            },
        },

        # ✅ configuración de precios (default + overrides por usuario)
        "pricing": {
            "default": {    # precios globales
                "CURP": 0,
                "RFC_IDCIF": 1,
                "QR": 1,
            },
            "users": {      # overrides por usuario
                # "528991234567": {"CURP": 100, "RFC_IDCIF": 60, "QR": 60}
            }
        },

        # dedupe global (antes era RFC; ahora es "key" para soportar CURP también)
        "ok_index": {},  # "RFC:ABC..." o "CURP:XXXX..." -> {"user": "...", "ts": "...", "type": "...", "price": 0}
        "rfc_ok_index": {},  # compat (si ya tenías data vieja)
        
        # bloqueos / allowlist
        "blocked_users": {},
        "allowlist_enabled": False,
        "allowlist_wa": [],
        "allowlist_meta": {},
    }

def _safe_read(path: str):
    try:
        if not os.path.exists(path):
            return _default_state()
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        data = _default_state()

    # ✅ migraciones suaves
    if "pricing" not in data:
        data["pricing"] = _default_state()["pricing"]
    else:
        data["pricing"].setdefault("default", _default_state()["pricing"]["default"])
        data["pricing"].setdefault("users", {})

    if "billing" not in data:
        data["billing"] = _default_state()["billing"]
    else:
        data["billing"].setdefault("total_billed", 0)
        data["billing"].setdefault("total_revenue_mxn", 0)
        data["billing"].setdefault("by_user", {})
        data["billing"].setdefault("by_type", _default_state()["billing"]["by_type"])

    # compat: si existía rfc_ok_index, lo dejamos, pero usamos ok_index
    data.setdefault("ok_index", {})
    data.setdefault("rfc_ok_index", {})  # viejo
    data.setdefault("attempts", {})
    data.setdefault("por_usuario", {})
    data.setdefault("por_dia", {})
    data.setdefault("last_success", [])
    data.setdefault("blocked_users", {})
    data.setdefault("allowlist_enabled", False)
    data.setdefault("allowlist_wa", [])
    data.setdefault("allowlist_meta", {})

    return data

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

# ================== STATS ==================

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
        info["success"] = 0

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

    rfcs_ok = info.setdefault("rfcs_ok", [])
    if rfc:
        rfcs_ok.append(rfc)
        if len(rfcs_ok) > MAX_RFC_HISTORY:
            del rfcs_ok[:-MAX_RFC_HISTORY]

    pu[user] = info

    last = state.setdefault("last_success", [])
    if rfc:
        last.append(rfc)
        if len(last) > 100:
            del last[:-100]

def log_attempt(state: dict, user_key: str, key: str | None, ok: bool, code: str, meta: dict | None = None, is_test: bool = False):
    user_key = user_key or "UNKNOWN"
    state.setdefault("attempts", {})
    state["attempts"].setdefault(user_key, [])

    entry = {
        "ts": _now_iso(),
        "key": (key or ""),
        "ok": bool(ok),
        "code": code,
        "meta": meta or {},
        "is_test": bool(is_test),
    }
    state["attempts"][user_key].append(entry)
    if len(state["attempts"][user_key]) > MAX_ATTEMPTS_PER_USER:
        state["attempts"][user_key] = state["attempts"][user_key][-MAX_ATTEMPTS_PER_USER:]

# ================== PRICING ==================

def get_pricing(state: dict) -> dict:
    p = state.setdefault("pricing", {})
    p.setdefault("default", {"CURP": 115, "RFC_IDCIF": 70, "QR": 70})
    p.setdefault("users", {})
    # asegurar llaves
    for k in INPUT_TYPES:
        p["default"].setdefault(k, 0)
    return p

def set_default_price(state: dict, input_type: str, price_mxn: int):
    input_type = (input_type or "").strip().upper()
    if input_type not in INPUT_TYPES:
        raise ValueError("INVALID_TYPE")
    p = get_pricing(state)
    p["default"][input_type] = int(price_mxn or 0)

def set_user_price(state: dict, user: str, input_type: str, price_mxn: int):
    user = (user or "").strip()
    input_type = (input_type or "").strip().upper()
    if not user:
        raise ValueError("EMPTY_USER")
    if input_type not in INPUT_TYPES:
        raise ValueError("INVALID_TYPE")
    p = get_pricing(state)
    p["users"].setdefault(user, {})
    p["users"][user][input_type] = int(price_mxn or 0)

def delete_user_price(state: dict, user: str, input_type: str | None = None):
    user = (user or "").strip()
    if not user:
        return
    p = get_pricing(state)
    if input_type:
        t = input_type.strip().upper()
        if t in (p["users"].get(user) or {}):
            p["users"][user].pop(t, None)
        if not (p["users"].get(user) or {}):
            p["users"].pop(user, None)
    else:
        p["users"].pop(user, None)

def resolve_price(state: dict, user: str, input_type: str) -> int:
    user = (user or "").strip()
    input_type = (input_type or "").strip().upper()
    if input_type not in INPUT_TYPES:
        input_type = "RFC_IDCIF"
    p = get_pricing(state)
    ov = (p.get("users") or {}).get(user) or {}
    if input_type in ov:
        return int(ov[input_type] or 0)
    return int((p.get("default") or {}).get(input_type) or 0)

# ================== BILLING / DEDUPE ==================

def _norm_ok_key(key: str) -> str:
    return (key or "").strip().upper()

def is_key_already_billed(state: dict, key: str) -> bool:
    k = _norm_ok_key(key)
    idx = state.get("ok_index") or {}
    if k and k in idx:
        return True
    # compat viejo
    old = state.get("rfc_ok_index") or {}
    return bool(k and k.replace("RFC:", "") in old)

def bill_success_if_new(state: dict, user: str, ok_key: str, input_type: str, price_mxn: int, is_test: bool = False) -> dict:
    """
    DEDUPE global por ok_key:
      - RFC:ABC...
      - CURP:XXXX...
    """
    user = user or "UNKNOWN"
    ok_key = _norm_ok_key(ok_key)
    input_type = (input_type or "").strip().upper()
    if input_type not in INPUT_TYPES:
        input_type = "RFC_IDCIF"

    if not ok_key:
        return {"billed": False, "reason": "EMPTY_KEY"}
    if is_test:
        return {"billed": False, "reason": "TEST"}

    state.setdefault("ok_index", {})
    idx = state["ok_index"]

    if ok_key in idx:
        return {"billed": False, "reason": "DUPLICATE"}

    idx[ok_key] = {"user": user, "ts": _now_iso(), "type": input_type, "price": int(price_mxn or 0)}

    billing = state.setdefault("billing", {})
    billing.setdefault("by_user", {})
    billing.setdefault("by_type", _default_state()["billing"]["by_type"])

    price = int(price_mxn or 0)

    billing["total_billed"] = int(billing.get("total_billed") or 0) + 1
    billing["total_revenue_mxn"] = int(billing.get("total_revenue_mxn") or 0) + price

    # global por tipo
    bt = billing["by_type"].setdefault(input_type, {"billed": 0, "revenue_mxn": 0})
    bt["billed"] = int(bt.get("billed") or 0) + 1
    bt["revenue_mxn"] = int(bt.get("revenue_mxn") or 0) + price
    billing["by_type"][input_type] = bt

    # por usuario
    by_user = billing["by_user"]
    u = by_user.get(user) or {"billed": 0, "revenue_mxn": 0, "by_type": {}, "keys": [], "last": ""}

    u["billed"] = int(u.get("billed") or 0) + 1
    u["revenue_mxn"] = int(u.get("revenue_mxn") or 0) + price
    u["last"] = _now_iso()

    u["by_type"].setdefault(input_type, {"billed": 0, "revenue_mxn": 0})
    u["by_type"][input_type]["billed"] = int(u["by_type"][input_type]["billed"]) + 1
    u["by_type"][input_type]["revenue_mxn"] = int(u["by_type"][input_type]["revenue_mxn"]) + price

    keys = list(u.get("keys") or [])
    keys.append(ok_key)
    if len(keys) > 200:
        keys = keys[-200:]
    u["keys"] = keys

    by_user[user] = u
    billing["by_user"] = by_user
    state["billing"] = billing

    return {"billed": True, "reason": "NEW_OK", "price": price, "type": input_type, "key": ok_key}

def unbill_key(state: dict, ok_key: str) -> dict:
    ok_key = _norm_ok_key(ok_key)
    if not ok_key:
        return {"ok": False, "reason": "EMPTY_KEY"}

    idx = state.get("ok_index") or {}
    if ok_key not in idx:
        return {"ok": True, "removed": False, "reason": "NOT_FOUND"}

    rec = idx.pop(ok_key, None) or {}
    owner = rec.get("user") or "UNKNOWN"
    price = int(rec.get("price") or 0)
    input_type = (rec.get("type") or "RFC_IDCIF").upper()

    billing = state.setdefault("billing", {})
    billing["total_billed"] = max(0, int(billing.get("total_billed") or 0) - 1)
    billing["total_revenue_mxn"] = max(0, int(billing.get("total_revenue_mxn") or 0) - price)

    # global tipo
    bt = (billing.get("by_type") or {}).get(input_type) or {"billed": 0, "revenue_mxn": 0}
    bt["billed"] = max(0, int(bt.get("billed") or 0) - 1)
    bt["revenue_mxn"] = max(0, int(bt.get("revenue_mxn") or 0) - price)
    billing.setdefault("by_type", {})[input_type] = bt

    by_user = billing.setdefault("by_user", {})
    u = by_user.get(owner) or {"billed": 0, "revenue_mxn": 0, "by_type": {}, "keys": [], "last": ""}

    u["billed"] = max(0, int(u.get("billed") or 0) - 1)
    u["revenue_mxn"] = max(0, int(u.get("revenue_mxn") or 0) - price)
    u["last"] = _now_iso()

    if input_type in (u.get("by_type") or {}):
        u["by_type"][input_type]["billed"] = max(0, int(u["by_type"][input_type]["billed"] or 0) - 1)
        u["by_type"][input_type]["revenue_mxn"] = max(0, int(u["by_type"][input_type]["revenue_mxn"] or 0) - price)

    u["keys"] = [k for k in (u.get("keys") or []) if _norm_ok_key(k) != ok_key]
    by_user[owner] = u
    billing["by_user"] = by_user
    state["billing"] = billing

    _remove_key_from_lists(state, ok_key)

    return {"ok": True, "removed": True, "owner": owner, "price": price, "type": input_type}

def _remove_key_from_lists(state: dict, ok_key: str):
    ok_key = _norm_ok_key(ok_key)
    if not ok_key:
        return

    # si es RFC:XXX también limpia last_success / rfcs_ok con el RFC puro
    rfc_plain = ok_key.replace("RFC:", "").strip()

    last = list(state.get("last_success") or [])
    state["last_success"] = [x for x in last if (x or "").upper().strip() != rfc_plain]

    pu = state.get("por_usuario") or {}
    for user, info in pu.items():
        if not isinstance(info, dict):
            continue
        rfcs_ok = list(info.get("rfcs_ok") or [])
        info["rfcs_ok"] = [x for x in rfcs_ok if (x or "").upper().strip() != rfc_plain]
        pu[user] = info
    state["por_usuario"] = pu

# ================== BLOQUEOS ==================

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

# ================== ALLOWLIST WA ==================

def _norm_wa(wa_id: str) -> str:
    import re
    return re.sub(r"\D", "", wa_id or "").strip()

def is_allowed(state: dict, wa_id: str) -> bool:
    wa_id = _norm_wa(wa_id)
    enabled = bool((state.get("allowlist_enabled") or False))
    if not enabled:
        return True
    allow = set((state.get("allowlist_wa") or []))
    return bool(wa_id and wa_id in allow)

def allow_add(state: dict, wa_id: str, note: str = ""):
    wa_id = _norm_wa(wa_id)
    if not wa_id:
        return
    state.setdefault("allowlist_wa", [])
    state.setdefault("allowlist_meta", {})

    allow = set(state["allowlist_wa"])
    allow.add(wa_id)
    state["allowlist_wa"] = sorted(list(allow))

    state["allowlist_meta"][wa_id] = {"ts": _now_iso(), "note": note or ""}

def allow_remove(state: dict, wa_id: str):
    wa_id = _norm_wa(wa_id)
    if not wa_id:
        return
    allow = set(state.get("allowlist_wa") or [])
    if wa_id in allow:
        allow.remove(wa_id)
        state["allowlist_wa"] = sorted(list(allow))
    (state.get("allowlist_meta") or {}).pop(wa_id, None)

def allow_set_enabled(state: dict, enabled: bool):
    state["allowlist_enabled"] = bool(enabled)

# cache_store.py
import json
import os
import time
import tempfile
import threading

# Ruta correcta (Render suele usar /app/data)
CACHE_FILE = (os.getenv("CACHE_FILE", "") or "/app/data/cache_checkid.json").strip()
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", str(30 * 24 * 3600)))  # 30 días

_THREAD_LOCK = threading.Lock()

def _ensure_dir():
    os.makedirs(os.path.dirname(CACHE_FILE) or ".", exist_ok=True)

def _load_cache_nolock():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
    except Exception as e:
        # Mueve corrupto a .bad.<ts>
        try:
            bad = f"{CACHE_FILE}.bad.{int(time.time())}"
            os.replace(CACHE_FILE, bad)
            print(f"CACHE CORRUPT -> moved to {bad}. Reason: {repr(e)}")
        except Exception:
            print("CACHE LOAD ERROR:", repr(e))
        return {}

def _atomic_write_json(path: str, obj: dict):
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)

    fd, tmp = tempfile.mkstemp(prefix=".tmp_cache_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # atomic swap
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

def _with_process_lock(fn, *args, **kwargs):
    """
    Lock entre procesos usando flock (solo Linux).
    Si flock falla por cualquier razón, igual ejecuta con thread lock.
    """
    try:
        import fcntl
        _ensure_dir()
        lock_path = CACHE_FILE + ".lock"
        with open(lock_path, "w") as lockf:
            fcntl.flock(lockf, fcntl.LOCK_EX)
            try:
                return fn(*args, **kwargs)
            finally:
                fcntl.flock(lockf, fcntl.LOCK_UN)
    except Exception:
        # fallback
        return fn(*args, **kwargs)

def cache_get(key: str):
    key = (key or "").strip()
    if not key:
        return None

    now = int(time.time())

    def _do():
        with _THREAD_LOCK:
            cache = _load_cache_nolock()
            item = cache.get(key)
            if not item or not isinstance(item, dict):
                return None

            ts = int(item.get("ts") or 0)
            if now - ts > CACHE_TTL:
                cache.pop(key, None)
                _atomic_write_json(CACHE_FILE, cache)
                return None

            return item.get("data")

    return _with_process_lock(_do)

def cache_set(key: str, data: dict):
    key = (key or "").strip()
    if not key:
        return

    def _do():
        with _THREAD_LOCK:
            cache = _load_cache_nolock()
            cache[key] = {"ts": int(time.time()), "data": data}
            _atomic_write_json(CACHE_FILE, cache)

    _with_process_lock(_do)

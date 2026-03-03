# cache_sqlite.py
import os, json, time, sqlite3, threading

DB_PATH = (os.getenv("CACHE_DB", "") or "/app/data/cache_checkid.sqlite").strip()
DEFAULT_TTL = int(os.getenv("CACHE_TTL_SEC", str(30 * 24 * 3600)))

_LOCK = threading.Lock()
_CONN = None

def _get_conn():
    global _CONN
    if _CONN is not None:
        return _CONN
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    c = sqlite3.connect(DB_PATH, timeout=10.0, isolation_level=None, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA synchronous=NORMAL;")
    c.execute("""
      CREATE TABLE IF NOT EXISTS cache (
        k TEXT PRIMARY KEY,
        exp INTEGER,
        ts INTEGER,
        data TEXT
      )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_cache_exp ON cache(exp)")
    _CONN = c
    return _CONN

def cache_get(key: str):
    key = (key or "").strip()
    if not key:
        return None
    now = int(time.time())
    with _LOCK:
        c = _get_conn()
        row = c.execute("SELECT data, exp, ts FROM cache WHERE k=?", (key,)).fetchone()
        if not row:
            return None
        data_s, exp, ts = row
        if exp is not None and now > int(exp):
            c.execute("DELETE FROM cache WHERE k=?", (key,))
            return None
        if exp is None and DEFAULT_TTL:
            if now - int(ts or 0) > int(DEFAULT_TTL):
                c.execute("DELETE FROM cache WHERE k=?", (key,))
                return None
        try:
            return json.loads(data_s) if data_s else None
        except Exception:
            return None

def cache_set(key: str, data: dict, ttl: int = None, ttl_seconds: int = None):
    key = (key or "").strip()
    if not key:
        return
    if ttl is None and ttl_seconds is not None:
        ttl = ttl_seconds

    exp = None
    if ttl is not None:
        try:
            ttl_i = int(ttl)
            if ttl_i > 0:
                exp = int(time.time()) + ttl_i
        except Exception:
            exp = None

    now = int(time.time())
    data_s = json.dumps(data, ensure_ascii=False)

    with _LOCK:
        c = _get_conn()
        c.execute(
            "INSERT INTO cache(k, exp, ts, data) VALUES(?,?,?,?) "
            "ON CONFLICT(k) DO UPDATE SET exp=excluded.exp, ts=excluded.ts, data=excluded.data",
            (key, exp, now, data_s)
        )

def cache_del(key: str):
    key = (key or "").strip()
    if not key:
        return
    with _LOCK:
        c = _get_conn()
        c.execute("DELETE FROM cache WHERE k=?", (key,))

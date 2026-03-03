import json, os, time
from cache_sqlite import cache_set

JSON_PATH = "/app/data/cache_checkid.json"

if not os.path.exists(JSON_PATH):
    print("No JSON cache found.")
    raise SystemExit(0)

with open(JSON_PATH, "r", encoding="utf-8") as f:
    obj = json.load(f)

n = 0
for k, v in (obj or {}).items():
    if not isinstance(v, dict):
        continue
    data = v.get("data")
    exp = v.get("exp")
    if not isinstance(data, dict):
        continue

    # si trae exp absolute, conviértelo a ttl
    ttl = None
    if exp is not None:
        try:
            ttl = max(1, int(exp) - int(time.time()))
        except Exception:
            ttl = None

    cache_set(k, data, ttl=ttl)
    n += 1

print("Migrated:", n)

"""Generate synthetic web/API request logs (clearly synthetic) as gzipped JSON Lines.
Three sources (web, api, cdn) write separate files, as in a multi-source log estate.
~1% of lines are deliberately corrupted so the data-quality gate has something to catch."""
import gzip, json, sys, os, random
import numpy as np
N = int(sys.argv[1]) if len(sys.argv) > 1 else 4_000_000
OUT = sys.argv[2] if len(sys.argv) > 2 else "data/raw"
DAYS = 30
rng = np.random.default_rng(42)
random.seed(42)
endpoints = ["/", "/search", "/product", "/cart", "/checkout", "/login", "/api/v1/items", "/api/v1/orders", "/api/v1/users", "/static/app.js", "/static/logo.png", "/health"]
ep_w = np.array([18, 14, 16, 8, 4, 5, 10, 5, 4, 8, 6, 2], float); ep_w /= ep_w.sum()
regions = ["eu-west-2", "eu-west-1", "us-east-1", "us-west-2", "ap-south-1"]
devices = ["desktop", "mobile", "tablet", "bot"]
sources = ["web", "api", "cdn"]
base_lat = {"/": 40, "/search": 120, "/product": 80, "/cart": 70, "/checkout": 150, "/login": 90, "/api/v1/items": 60, "/api/v1/orders": 110, "/api/v1/users": 55, "/static/app.js": 15, "/static/logo.png": 10, "/health": 3}
os.makedirs(OUT, exist_ok=True)
day = rng.integers(0, DAYS, N)
sec = rng.integers(0, 86400, N)
ep = rng.choice(len(endpoints), N, p=ep_w)
user = rng.zipf(1.3, N) % 200_000
region = rng.choice(len(regions), N, p=[.35, .15, .25, .15, .10])
dev = rng.choice(len(devices), N, p=[.5, .38, .08, .04])
src = rng.choice(len(sources), N, p=[.5, .3, .2])
lat = np.array([base_lat[e] for e in endpoints])[ep] * rng.lognormal(0, .5, N)
status = rng.choice([200, 301, 404, 500, 503], N, p=[.93, .02, .035, .01, .005])
lat = np.where(status >= 500, lat * 4, lat)
byt = (rng.lognormal(7, 1.1, N)).astype(int)
req_id = np.arange(N)
handles = {s: gzip.open(f"{OUT}/{s}_logs.jsonl.gz", "wt", compresslevel=3) for s in sources}
bad = 0
for i in range(N):
    ts = f"2026-10-{(day[i] % 30) + 1:02d}T{sec[i] // 3600:02d}:{(sec[i] % 3600) // 60:02d}:{sec[i] % 60:02d}Z"
    rec = {"request_id": int(req_id[i]), "ts": ts, "user_id": int(user[i]), "endpoint": endpoints[ep[i]], "status": int(status[i]),
           "latency_ms": round(float(lat[i]), 2), "bytes": int(byt[i]), "region": regions[region[i]], "device": devices[dev[i]], "source": sources[src[i]]}
    r = random.random()
    if r < 0.003:   # malformed line
        handles[rec["source"]].write('{"request_id": ' + str(rec["request_id"]) + ', "ts": "' + ts + '", "user_id"\n'); bad += 1; continue
    if r < 0.006:   # null / impossible values
        rec["latency_ms"] = -1 if r < 0.0045 else None; bad += 1
    elif r < 0.008: # duplicate event (same request_id written twice)
        handles[rec["source"]].write(json.dumps(rec) + "\n"); bad += 1
    handles[rec["source"]].write(json.dumps(rec) + "\n")
for h in handles.values(): h.close()
print(json.dumps({"events": N, "injected_bad_records": bad}))
open(f"{OUT}/_injected.json", "w").write(json.dumps({"events": N, "injected_bad_records": bad}))

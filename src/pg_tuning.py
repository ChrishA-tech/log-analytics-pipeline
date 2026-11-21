"""Query optimisation on ~4M rows in PostgreSQL 16: untuned heap table vs tuned (daily range partitions, composite/partial indexes, rollup).
Each query: EXPLAIN (ANALYZE) execution time, median of 5 warm runs."""
import psycopg2, duckdb, json, statistics, os, io
admin = psycopg2.connect(host="/tmp", port=5433, user="postgres", dbname="postgres"); admin.autocommit = True
admin.cursor().execute("DROP DATABASE IF EXISTS logs"); admin.cursor().execute("CREATE DATABASE logs"); admin.close()
db = psycopg2.connect(host="/tmp", port=5433, user="postgres", dbname="logs"); db.autocommit = True; cur = db.cursor()
duckdb.connect().execute("COPY (SELECT request_id, ts, user_id, endpoint, status, latency_ms, bytes, region, device, source FROM read_parquet('data/curated/requests/*/*.parquet')) TO 'data/pg_load.csv' (HEADER false)")
DDL = "(request_id bigint, ts timestamp, user_id bigint, endpoint text, status int, latency_ms double precision, bytes bigint, region text, device text, source text)"
cur.execute(f"CREATE TABLE requests_base {DDL}")
with open("data/pg_load.csv") as f: cur.copy_expert("COPY requests_base FROM STDIN WITH CSV", f)
cur.execute("ANALYZE requests_base")
# tuned design: daily range partitions + local indexes + partial index + rollup
cur.execute(f"CREATE TABLE requests {DDL} PARTITION BY RANGE (ts)")
for d in range(1, 31):
    cur.execute(f"CREATE TABLE requests_d{d:02d} PARTITION OF requests FOR VALUES FROM ('2026-10-{d:02d}') TO ('{'2026-10-%02d' % (d+1) if d < 30 else '2026-11-01'}')")
cur.execute("INSERT INTO requests SELECT * FROM requests_base")
cur.execute("CREATE INDEX ON requests (endpoint, ts)"); cur.execute("CREATE INDEX ON requests (region, ts) INCLUDE (user_id)")
cur.execute("CREATE INDEX ON requests (ts) WHERE status >= 500"); cur.execute("CREATE INDEX ON requests (request_id)")
cur.execute("CREATE MATERIALIZED VIEW daily_rollup AS SELECT date_trunc('day', ts) AS day, endpoint, count(*) AS requests, sum((status>=500)::int) AS errors, avg(latency_ms) AS avg_ms FROM requests GROUP BY 1,2")
cur.execute("ANALYZE requests"); cur.execute("ANALYZE daily_rollup")
Q = {
 "Q1 error rate per endpoint, 3 days": ("SELECT endpoint, count(*) FILTER (WHERE status>=500)::float/count(*) FROM {t} WHERE ts >= '2026-10-10' AND ts < '2026-10-13' GROUP BY endpoint", None),
 "Q2 top 10 users in a region, 1 week": ("SELECT user_id, count(*) c FROM {t} WHERE region='eu-west-2' AND ts >= '2026-10-05' AND ts < '2026-10-12' GROUP BY user_id ORDER BY c DESC LIMIT 10", None),
 "Q3 hourly p95 latency of /checkout, 1 week": ("SELECT date_trunc('hour', ts) h, percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FROM {t} WHERE endpoint='/checkout' AND ts >= '2026-10-05' AND ts < '2026-10-12' GROUP BY 1 ORDER BY 1", None),
 "Q4 single request lookup by request_id": ("SELECT * FROM {t} WHERE request_id = 2718281 AND ts >= '2026-10-01' AND ts < '2026-11-01'", None),
 "Q5 dashboard: daily volume and 5xx, whole month": ("SELECT date_trunc('day', ts) d, count(*), sum((status>=500)::int) FROM {t} GROUP BY 1 ORDER BY 1",
                                                      "SELECT day, sum(requests), sum(errors) FROM daily_rollup GROUP BY 1 ORDER BY 1"),
}
def run(sql):
    ts = []
    for _ in range(6):
        cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql); ts.append(cur.fetchone()[0][0]["Execution Time"])
    return statistics.median(ts[1:])
def plan(sql):
    cur.execute("EXPLAIN " + sql); return [r[0] for r in cur.fetchall()][:3]
out = {}
for name, (sql, opt_sql) in Q.items():
    b = run(sql.format(t="requests_base")); o = run((opt_sql or sql.format(t="requests")))
    out[name] = {"baseline_ms": round(b, 1), "tuned_ms": round(o, 1), "speedup_x": round(b / o, 1), "tuned_plan_head": plan(opt_sql or sql.format(t="requests"))}
    print(name, out[name]["baseline_ms"], "->", out[name]["tuned_ms"], f"({out[name]['speedup_x']}x)")
cur.execute("SELECT count(*) FROM requests_base"); out["rows"] = cur.fetchone()[0]
json.dump(out, open("data/pg_result.json", "w"), indent=1)

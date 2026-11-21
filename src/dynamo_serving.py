"""Serving layer: hourly per-endpoint aggregates in DynamoDB (emulated locally with moto; same boto3 API as AWS DynamoDB).
Key design: partition key = endpoint, sort key = hour (ISO string) -> one Query returns a contiguous time range for one endpoint."""
import duckdb, json, time, statistics
from common import resource, client
con = duckdb.connect()
rows = con.execute("""SELECT endpoint, strftime(date_trunc('hour', ts), '%Y-%m-%dT%H') AS hour, count(*) AS requests,
   sum((status>=500)::int) AS errors, round(quantile_cont(latency_ms, 0.95), 1) AS p95_ms
   FROM read_parquet('data/curated/requests/*/*.parquet') GROUP BY 1,2""").fetchall()
ddb = client("dynamodb")
ddb.create_table(TableName="endpoint_hourly", BillingMode="PAY_PER_REQUEST",
    KeySchema=[{"AttributeName": "endpoint", "KeyType": "HASH"}, {"AttributeName": "hour", "KeyType": "RANGE"}],
    AttributeDefinitions=[{"AttributeName": "endpoint", "AttributeType": "S"}, {"AttributeName": "hour", "AttributeType": "S"}])
tbl = resource("dynamodb").Table("endpoint_hourly")
t = time.perf_counter()
with tbl.batch_writer() as bw:
    for ep, h, n, e, p in rows: bw.put_item(Item={"endpoint": ep, "hour": h, "requests": n, "errors": e, "p95_ms": str(p)})
t_load = time.perf_counter() - t
lat = []
for i in range(300):
    ep = ["/checkout", "/search", "/api/v1/orders"][i % 3]
    s = time.perf_counter()
    r = tbl.query(KeyConditionExpression="endpoint = :e AND #h BETWEEN :a AND :b", ExpressionAttributeNames={"#h": "hour"},
        ExpressionAttributeValues={":e": ep, ":a": "2026-10-10T00", ":b": "2026-10-12T23"})
    lat.append((time.perf_counter() - s) * 1000)
res = {"items": len(rows), "load_seconds": round(t_load, 1), "items_per_query": r["Count"], "queries": 300, "p50_ms": round(statistics.median(lat), 1), "note": "moto emulation, not an AWS performance figure"}
print(json.dumps(res)); json.dump(res, open("data/dynamo_result.json", "w"))

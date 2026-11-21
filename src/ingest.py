"""Land raw multi-source logs in an S3 data lake (emulated locally with moto; same boto3 API as AWS S3).
Layout: s3://log-lake/raw/source=<src>/ingest_date=<d>/<file>"""
import glob, os, json
from common import client, BUCKET, Timer
s3 = client("s3")
s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
n = 0; size = 0
with Timer() as t:
    for f in sorted(glob.glob("data/raw/*_logs.jsonl.gz")):
        src = os.path.basename(f).split("_")[0]
        key = f"raw/source={src}/ingest_date=2026-11-01/{os.path.basename(f)}"
        s3.upload_file(f, BUCKET, key); n += 1; size += os.path.getsize(f)
print(json.dumps({"objects": n, "bytes": size, "seconds": round(t.s, 2)}))

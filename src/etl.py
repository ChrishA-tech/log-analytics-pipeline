"""PySpark batch ETL: S3 raw -> validate -> quarantine bad rows -> dedupe -> Parquet (partitioned by day) -> S3 curated."""
import os, json, glob, shutil, sys
from pyspark.sql import SparkSession, functions as F, types as T
from common import client, BUCKET, Timer
os.makedirs("data/stage", exist_ok=True)
s3 = client("s3")
for o in s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")["Contents"]:
    dst = "data/stage/" + o["Key"]; os.makedirs(os.path.dirname(dst), exist_ok=True); s3.download_file(BUCKET, o["Key"], dst)
spark = (SparkSession.builder.master("local[2]").appName("log-etl").config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "16").config("spark.ui.enabled", "false").getOrCreate())
schema = T.StructType([T.StructField("request_id", T.LongType()), T.StructField("ts", T.StringType()), T.StructField("user_id", T.LongType()),
    T.StructField("endpoint", T.StringType()), T.StructField("status", T.IntegerType()), T.StructField("latency_ms", T.DoubleType()),
    T.StructField("bytes", T.LongType()), T.StructField("region", T.StringType()), T.StructField("device", T.StringType()),
    T.StructField("source", T.StringType()), T.StructField("_corrupt", T.StringType())])
with Timer() as t:
    raw = (spark.read.schema(schema).option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt").json("data/stage/raw/").cache())
    total = raw.count()
    malformed = raw.filter(F.col("_corrupt").isNotNull())
    parsed = raw.filter(F.col("_corrupt").isNull()).drop("_corrupt")
    invalid = parsed.filter(F.col("latency_ms").isNull() | (F.col("latency_ms") < 0))
    valid = parsed.filter(F.col("latency_ms").isNotNull() & (F.col("latency_ms") >= 0))
    dedup = valid.dropDuplicates(["request_id"])
    n_mal, n_inv = malformed.count(), invalid.count()
    n_valid = valid.count(); n_clean = dedup.count(); n_dup = n_valid - n_clean
    clean = dedup.withColumn("ts", F.to_timestamp("ts")).withColumn("dt", F.to_date("ts"))
    shutil.rmtree("data/curated", ignore_errors=True)
    clean.repartition("dt").write.partitionBy("dt").mode("overwrite").parquet("data/curated/requests")
    q = malformed.select("_corrupt").union(invalid.select(F.to_json(F.struct(*[c for c in invalid.columns if c != "_corrupt"])).alias("_corrupt")))
    shutil.rmtree("data/quarantine", ignore_errors=True)
    q.write.mode("overwrite").json("data/quarantine")
inj = json.load(open("data/raw/_injected.json"))
res = {"events_in": total, "malformed": n_mal, "invalid_latency": n_inv, "duplicates": n_dup, "clean_rows": n_clean,
       "detected_bad": n_mal + n_inv + n_dup, "injected_bad": inj["injected_bad_records"], "etl_seconds": round(t.s, 1)}
print(json.dumps(res)); json.dump(res, open("data/etl_result.json", "w"))
n = 0
for f in glob.glob("data/curated/requests/**/*.parquet", recursive=True):
    key = "curated/" + os.path.relpath(f, "data/curated"); s3.upload_file(f, BUCKET, key); n += 1
print("uploaded curated objects", n)
spark.stop()

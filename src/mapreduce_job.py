"""Same aggregation three ways, results must match: (1) Hadoop-Streaming-style mapper | sort | reducer, (2) PySpark RDD map/reduceByKey, (3) SQL (DuckDB) as the reference."""
import subprocess, json, time, duckdb, os
from pyspark.sql import SparkSession
con = duckdb.connect()
con.execute("COPY (SELECT endpoint, status, latency_ms FROM read_parquet('data/curated/requests/*/*.parquet')) TO 'data/mr_input.tsv' (DELIMITER '\t', HEADER false)")
t = time.perf_counter()
out = subprocess.run("cat data/mr_input.tsv | python3 src/mapper.py | LC_ALL=C sort | python3 src/reducer.py", shell=True, capture_output=True, text=True, check=True).stdout
t_stream = time.perf_counter() - t
stream = {l.split("\t")[0]: int(l.split("\t")[1]) for l in out.strip().split("\n")}
spark = SparkSession.builder.master("local[2]").config("spark.driver.memory", "3g").config("spark.ui.enabled", "false").getOrCreate()
t = time.perf_counter()
rdd = (spark.sparkContext.textFile("data/mr_input.tsv").map(lambda l: l.split("\t")).map(lambda p: (f"{p[0]}|{p[1][0]}xx", 1)).reduceByKey(lambda a, b: a + b))
sp = dict(rdd.collect()); t_spark = time.perf_counter() - t
ref = dict(con.execute("SELECT endpoint||'|'||substr(CAST(status AS VARCHAR),1,1)||'xx', count(*) FROM read_parquet('data/curated/requests/*/*.parquet') GROUP BY 1").fetchall())
res = {"keys": len(stream), "streaming_equals_spark": stream == sp, "streaming_equals_sql": stream == ref,
       "streaming_seconds": round(t_stream, 1), "spark_rdd_seconds": round(t_spark, 1), "rows": sum(stream.values())}
print(json.dumps(res)); json.dump(res, open("data/mapreduce_result.json", "w")); spark.stop()

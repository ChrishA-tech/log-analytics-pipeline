# Clickstream Log Analytics Pipeline

End-to-end log pipeline on **4.0M synthetic web/API/CDN events**: S3 raw zone, PySpark ETL with data-quality quarantine, Hadoop-Streaming-style MapReduce, DynamoDB serving table, PostgreSQL query tuning, dashboard extracts.

## Honest scope
- Data is **synthetic** (`src/gen_logs.py`), with 31,898 deliberately injected bad records (malformed, null/negative latency, duplicates).
- **S3 and DynamoDB are emulated locally with `moto`** and accessed through the real `boto3` API. Timings are not AWS performance figures. Pointing `src/common.py` at real AWS needs only a changed endpoint and credentials.
- **MapReduce** is a Hadoop-Streaming-style `mapper | sort | reducer`, run locally, not on a Hadoop cluster. Cross-checked against Spark RDD and SQL.
- **Dashboard**: plotly HTML plus CSV extracts in `extracts/`, ready to load into Tableau Public or QuickSight. No Tableau/QuickSight workbook is included.

## Stages
| Stage | File | Result |
|---|---|---|
| Generate | gen_logs.py | 4,007,970 lines |
| Ingest to S3 | ingest.py | 3 objects, 101.9 MB |
| ETL (Spark) | etl.py | 3,976,072 clean rows; 31,898/31,898 bad records caught |
| MapReduce | mapper.py, reducer.py, mapreduce_job.py | matches Spark and SQL on all 48 keys |
| DynamoDB | dynamo_serving.py | 8,640 items, PK endpoint / SK hour |
| Query tuning | pg_tuning.py | see below |
| Extracts | extracts.py | CSVs + dashboard.html |

## PostgreSQL 16 tuning (median of 5 warm EXPLAIN ANALYZE runs, 3.98M rows)
Baseline: unindexed heap. Tuned: daily range partitions, composite, covering and partial indexes, materialised-view rollup.
Speedups per query are in `data/pg_result.json`; point lookup and dashboard rollup improve by orders of magnitude, scan-heavy aggregates by about 2x to 5x.

## Run
`pip install pyspark duckdb moto[server] boto3 pyarrow psycopg2-binary plotly pytest`, start `moto_server -p 5000` and PostgreSQL, then `bash run_all.sh`. Needs Java 17+.

## Optional: real AWS and Hadoop cluster
- `aws/CHECKLIST.md`: run the same boto3 code against real S3, DynamoDB and Athena.
- `hadoop/`: 3-node HDFS + YARN cluster in Docker; `run_streaming.sh` runs the same mapper/reducer via Hadoop Streaming (untested template).

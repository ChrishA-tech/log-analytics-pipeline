set -e
python3 src/gen_logs.py; python3 src/ingest.py; python3 src/etl.py; python3 src/mapreduce_job.py
python3 src/dynamo_serving.py; python3 src/pg_tuning.py; python3 src/extracts.py; python3 -m pytest -q tests

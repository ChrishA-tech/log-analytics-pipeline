#!/bin/bash
# Run from repo root after: docker compose -f hadoop/docker-compose.yml up -d
# data/mr_input.tsv is produced by src/mapreduce_job.py (endpoint<TAB>status<TAB>latency_ms).
set -e
C="docker compose -f hadoop/docker-compose.yml exec -T namenode"
docker compose -f hadoop/docker-compose.yml cp data/mr_input.tsv namenode:/tmp/mr_input.tsv
docker compose -f hadoop/docker-compose.yml cp src/mapper.py namenode:/tmp/mapper.py
docker compose -f hadoop/docker-compose.yml cp src/reducer.py namenode:/tmp/reducer.py
$C hdfs dfs -mkdir -p /logs && $C hdfs dfs -put -f /tmp/mr_input.tsv /logs/
$C hdfs dfs -rm -r -f /out
time $C hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /tmp/mapper.py,/tmp/reducer.py -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
  -input /logs/mr_input.tsv -output /out
$C hdfs dfs -cat /out/part-* | sort > data/hadoop_out.tsv
echo "compare data/hadoop_out.tsv with the output of src/mapreduce_job.py (48 keys)"

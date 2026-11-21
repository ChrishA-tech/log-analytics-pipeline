import json, subprocess, sys
def test_etl_detects_every_injected_bad_record():
    r = json.load(open("data/etl_result.json")); assert r["detected_bad"] == r["injected_bad"]
    assert r["clean_rows"] + r["detected_bad"] == r["events_in"]
def test_mapreduce_matches_spark_and_sql():
    r = json.load(open("data/mapreduce_result.json")); assert r["streaming_equals_spark"] and r["streaming_equals_sql"]
def test_mapper_reducer_roundtrip():
    line = "/a\t500\t10.0"
    m = subprocess.run([sys.executable, "src/mapper.py"], input=line + "\n", capture_output=True, text=True).stdout
    out = subprocess.run([sys.executable, "src/reducer.py"], input=m, capture_output=True, text=True).stdout
    assert out.strip()
def test_pg_tuning_faster():
    r = json.load(open("data/pg_result.json"))
    for k, v in r.items():
        if isinstance(v, dict): assert v["tuned_ms"] < v["baseline_ms"], k

#!/usr/bin/env python3
"""Hadoop-Streaming-style mapper: reads 'endpoint<TAB>status<TAB>latency_ms' lines, emits '<endpoint>|<status_class><TAB>1'."""
import sys
for line in sys.stdin:
    ep, status, _ = line.rstrip("\n").split("\t")
    print(f"{ep}|{status[0]}xx\t1")

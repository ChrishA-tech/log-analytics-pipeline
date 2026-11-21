#!/usr/bin/env python3
"""Streaming-style reducer: input sorted by key (the shuffle), sums counts per key."""
import sys
cur, total = None, 0
for line in sys.stdin:
    k, v = line.rstrip("\n").split("\t")
    if k != cur:
        if cur is not None: print(f"{cur}\t{total}")
        cur, total = k, 0
    total += int(v)
if cur is not None: print(f"{cur}\t{total}")

#!/usr/bin/env bash
set -euo pipefail
mkdir -p data/normalized
docker run --rm -u $(id -u):$(id -g) -v "$PWD/data/input":/in -v "$PWD/data/normalized":/out -e LOG_LEVEL=${LOG_LEVEL:-INFO} python:3.11-slim bash -lc '  pip install --no-cache-dir orjson &&   python - <<"PY"
import os, orjson, json, hashlib, time, sys
from pathlib import Path
IN=Path("/in"); OUT=Path("/out")
nodes=set(); edges=[]
def nid(s): 
    return hashlib.sha1(s.encode()).hexdigest()
for p in IN.rglob("*.json"):
    try:
        data=json.loads(p.read_text())
        if isinstance(data, dict): data=[data]
        for rec in data:
            src=str(rec.get("source") or rec.get("src") or rec.get("from") or rec.get("input") or "")
            dst=str(rec.get("target") or rec.get("dst") or rec.get("to") or rec.get("output") or "")
            if not src or not dst: 
                continue
            op=rec.get("op") or rec.get("operation") or "flow"
            repo=rec.get("repo") or rec.get("repository")
            ts=rec.get("ts") or rec.get("timestamp") or time.time()
            s_id=nid(src); d_id=nid(dst)
            nodes.add((s_id,src,rec.get("src_type") or "entity", repo))
            nodes.add((d_id,dst,rec.get("dst_type") or "entity", repo))
            edges.append({"src":s_id,"dst":d_id,"op":op,"ts":ts,"weight":1.0})
    except Exception as e:
        print(f"[WARN] skip {p}: {e}", file=sys.stderr)
OUT.mkdir(parents=True, exist_ok=True)
with open(OUT/"nodes.jsonl","wb") as f:
    for n in nodes:
        f.write(orjson.dumps({"id":n[0],"name":n[1],"type":n[2],"repo":n[3]})+b"\n")
with open(OUT/"edges.jsonl","wb") as f:
    for e in edges:
        f.write(orjson.dumps(e)+b"\n")
print(f"Wrote {len(nodes)} nodes and {len(edges)} edges to /out")
PY'

#!/usr/bin/env python3
import argparse, orjson, os, sys
import pyTigerGraph as tg

def load_lines(path):
    with open(path, "rb") as f:
        for line in f:
            if line.strip():
                yield orjson.loads(line)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", required=True)
    ap.add_argument("--edges", required=True)
    args = ap.parse_args()
    host=os.getenv("TG_HOST","http://tigergraph:9000").rstrip("/")
    graph=os.getenv("TG_GRAPH","LineageGraph")
    user=os.getenv("TG_USERNAME","tigergraph")
    pwd=os.getenv("TG_PASSWORD","tigergraph")
    conn=tg.TigerGraphConnection(host=host, graphname=graph, username=user, password=pwd)
    try:
        conn.getToken(timeout=1440)
    except Exception as e:
        print("Failed to get token:", e, file=sys.stderr)
        sys.exit(1)
    vc=0
    for n in load_lines(args.nodes):
        conn.upsertVertex("Node", n["id"], {k:v for k,v in n.items() if k!="id"})
        vc+=1
    ec=0
    for e in load_lines(args.edges):
        conn.upsertEdge("Node", e["src"], "FLOWS_TO", "Node", e["dst"], {k:v for k,v in e.items() if k not in ("src","dst")})
        ec+=1
    print(f"TigerGraph upserted {vc} vertices and {ec} edges.")
if __name__=="__main__":
    main()

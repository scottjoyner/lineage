#!/usr/bin/env python3
import os, argparse, pandas as pd
from neo4j import GraphDatabase
import pyTigerGraph as tg

def neo4j_sinks(uri, user, pwd, src_ids):
    q = """
    MATCH (s:Node)-[:FLOWS_TO*]->(t:Node)
    WHERE s.id IN $SRC AND NOT (t)-[:FLOWS_TO]->()
    RETURN s.id AS source, collect(DISTINCT t.id) AS sinks
    """
    drv = GraphDatabase.driver(uri, auth=(user, pwd))
    with drv.session() as s:
        rows = s.run(q, SRC=src_ids).data()
    return {r["source"]: set(r["sinks"]) for r in rows}

def tg_sinks(host, graph, user, pwd, src_ids):
    conn = tg.TigerGraphConnection(host=host, graphname=graph, username=user, password=pwd)
    conn.getToken(timeout=1440)
    res = conn.runInstalledQuery("getSinks", params={"srcIds": list(set(src_ids))})
    sinks = set(res[0].get("sinks", [])) if res else set()
    return {sid: sinks for sid in src_ids}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = args.out
    os.makedirs(out, exist_ok=True)

    n_uri=os.getenv("NEO4J_URI","bolt://neo4j:7687")
    n_user=os.getenv("NEO4J_USER","neo4j")
    n_pwd=os.getenv("NEO4J_PASS","neo4j")

    t_host=os.getenv("TG_HOST","http://tigergraph:9000")
    t_graph=os.getenv("TG_GRAPH","LineageGraph")
    t_user=os.getenv("TG_USERNAME","tigergraph")
    t_pwd=os.getenv("TG_PASSWORD","tigergraph")

    drv = GraphDatabase.driver(n_uri, auth=(n_user,n_pwd))
    with drv.session() as s:
        src_ids = s.run("MATCH (s:Node)-[:FLOWS_TO]->() RETURN DISTINCT s.id AS id LIMIT 1000").value()

    n = neo4j_sinks(n_uri,n_user,n_pwd,src_ids)
    t = tg_sinks(t_host,t_graph,t_user,t_pwd,src_ids)

    rows = []
    for sid in src_ids:
        ns = n.get(sid,set()); ts = t.get(sid,set())
        rows.append({
            "source": sid,
            "neo4j_sinks": len(ns),
            "tigergraph_sinks": len(ts),
            "only_in_neo4j": len(ns - ts),
            "only_in_tigergraph": len(ts - ns),
            "intersection": len(ns & ts)
        })
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(out, "comparison.csv"), index=False)
    with open(os.path.join(out,"comparison.md"), "w") as f:
        diff = int((df["only_in_neo4j"] + df["only_in_tigergraph"] > 0).sum())
        same = len(df) - diff
        f.write(f"# Lineage Comparison\n\nTotal sources: **{len(df)}**\n\nDiffering: {diff} • Exact matches: {same}\n")

    print("Comparison complete. Outputs at", out)

if __name__ == "__main__":
    main()

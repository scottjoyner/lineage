
#!/usr/bin/env python3
"""
Lineage comparator: executes parity queries on Neo4j and TigerGraph,
computes identical metrics from each backend's adjacency, writes CSV/MD/HTML,
emits Prometheus metrics, and (optionally) publishes artifacts to S3/GCS.
"""
from __future__ import annotations
import os, argparse, time, json, math, datetime, sys
from collections import defaultdict, deque
from typing import Dict, Set, Tuple, Iterable, List
import pandas as pd
from neo4j import GraphDatabase, Driver
import pyTigerGraph as tg
from jinja2 import Environment, FileSystemLoader, select_autoescape
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, push_to_gateway
from publish import build_publisher_from_env, Artifact, PublishError

# ---------------- Graph algorithms ----------------
def bfs_metrics(adj: Dict[str, Set[str]], src: str, max_depth: int) -> Tuple[Set[str], Dict[str,int], int]:
    dist: Dict[str,int] = {src: 0}
    q: deque[str] = deque([src])
    while q:
        u = q.popleft()
        if dist[u] >= max_depth: 
            continue
        for v in adj.get(u, ()):
            if v not in dist:
                dist[v] = dist[u] + 1
                q.append(v)
    reachable = set(dist.keys()) - {src}
    max_hops = max(dist.values()) if dist else 0
    return reachable, dist, max_hops

def detect_cycle(adj_subset: Dict[str, Iterable[str]]) -> bool:
    indeg = defaultdict(int)
    nodes: Set[str] = set()
    for u, nbrs in adj_subset.items():
        nodes.add(u)
        for v in nbrs:
            nodes.add(v)
            indeg[v] += 1
            indeg.setdefault(u, 0)
    q = deque([n for n in nodes if indeg[n] == 0])
    count = 0
    while q:
        u = q.popleft()
        count += 1
        for v in adj_subset.get(u, ()):
            indeg[v] -= 1
            if indeg[v] == 0:
                q.append(v)
    return count != len(nodes)

def bounded_path_count(adj: Dict[str, Set[str]], src: str, sinks: Set[str], max_depth: int) -> int | None:
    reach, dist, _ = bfs_metrics(adj, src, max_depth)
    nodes = {src} | reach
    sub = {u: [v for v in adj.get(u, ()) if v in nodes and dist.get(u, math.inf) < dist.get(v, math.inf) <= max_depth] for u in nodes}
    if detect_cycle(sub):
        return None
    topo = sorted(nodes, key=lambda x: dist.get(x, 0))
    ways = defaultdict(int); ways[src] = 1
    for u in topo:
        for v in sub.get(u, ()):
            ways[v] += ways[u]
    return int(sum(ways[s] for s in sinks if s in ways))

# ---------------- Backend accessors ----------------
def neo4j_get_edges(driver: Driver, src: str, max_depth: int) -> Dict[str, Set[str]]:
    q = """
    MATCH p=(s:Node {id:$SRC})-[:FLOWS_TO*..$D]->(t:Node)
    WITH DISTINCT relationships(p) AS rels
    UNWIND rels AS r
    RETURN DISTINCT startNode(r).id AS src, endNode(r).id AS dst
    """
    with driver.session() as s:
        rows = s.run(q, SRC=src, D=max_depth).data()
    adj: Dict[str, Set[str]] = defaultdict(set)
    for r in rows:
        adj[r["src"]].add(r["dst"])
    return adj

def neo4j_sinks(driver: Driver, src: str) -> Set[str]:
    q = """
    MATCH (s:Node {id:$SRC})-[:FLOWS_TO*]->(t:Node)
    WHERE NOT (t)-[:FLOWS_TO]->()
    RETURN collect(DISTINCT t.id) AS sinks
    """
    with driver.session() as s:
        out = s.run(q, SRC=src).single()
    return set(out["sinks"] or [])

def tg_get_edges(host: str, graph: str, user: str, pwd: str, src: str, max_depth: int) -> Dict[str, Set[str]]:
    conn = tg.TigerGraphConnection(host=host, graphname=graph, username=user, password=pwd)
    conn.getToken(timeout=1440)
    res = conn.runInstalledQuery("getEdgesWithinDepth", params={"srcIds":[src], "maxDepth": int(max_depth)})
    edges = set()
    if res and "edges" in res[0]:
        for e in res[0]["edges"]:
            try:
                u,v = e.split("|",1)
                edges.add((u,v))
            except Exception:
                pass
    adj: Dict[str, Set[str]] = defaultdict(set)
    for u,v in edges:
        adj[u].add(v)
    return adj

def tg_sinks(host: str, graph: str, user: str, pwd: str, src: str) -> Set[str]:
    conn = tg.TigerGraphConnection(host=host, graphname=graph, username=user, password=pwd)
    conn.getToken(timeout=1440)
    res = conn.runInstalledQuery("getSinks", params={"srcIds":[src]})
    return set(res[0].get("sinks", [])) if res else set()

# ---------------- Reporting helpers ----------------
def render_html(df: pd.DataFrame, out_html: str, max_depth: int, sources_limit: int) -> None:
    env = Environment(
        loader=FileSystemLoader("/app/templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template("report.html")
    df2 = df.copy()
    for col in ["eq_sinks","eq_reachable","eq_max_depth","eq_path_count"]:
        df2[col] = df2[col].map(lambda x: f"<span class='ok'>✓</span>" if x else f"<span class='bad'>✗</span>")
    table_html = df2.to_html(escape=False, index=False)
    html = tpl.render(
        generated_at=datetime.datetime.utcnow().isoformat()+"Z",
        total_sources=len(df),
        differences=int((~(df[["eq_sinks","eq_reachable","eq_max_depth","eq_path_count"]]).all(axis=1)).sum()),
        exact_parity=int((df[["eq_sinks","eq_reachable","eq_max_depth","eq_path_count"]].all(axis=1)).sum()),
        neo4j_sinks_total=int(df["neo4j_sinks"].sum()),
        tg_sinks_total=int(df["tigergraph_sinks"].sum()),
        max_depth=max_depth,
        sources_limit=sources_limit,
        table_html=table_html
    )
    with open(out_html, "w") as f:
        f.write(html)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="Output directory for reports")
    args = ap.parse_args()
    outdir = args.out
    os.makedirs(outdir, exist_ok=True)

    n_uri=os.getenv("NEO4J_URI","bolt://neo4j:7687")
    n_user=os.getenv("NEO4J_USER","neo4j")
    n_pwd=os.getenv("NEO4J_PASS","neo4j")
    t_host=os.getenv("TG_HOST","http://tigergraph:9000")
    t_graph=os.getenv("TG_GRAPH","LineageGraph")
    t_user=os.getenv("TG_USERNAME","tigergraph")
    t_pwd=os.getenv("TG_PASSWORD","tigergraph")
    max_depth=int(os.getenv("MAX_DEPTH","6"))
    src_limit=int(os.getenv("SOURCES_LIMIT","100"))
    pg=os.getenv("PUSHGATEWAY_URL")

    reg = CollectorRegistry()
    runs = Counter("lineage_compare_runs_total","Comparator runs", registry=reg)
    sources_g = Gauge("lineage_compare_sources_total","Sources per run", registry=reg)
    mismatches = Counter("lineage_compare_mismatches_total","Per-run mismatches (any metric)", registry=reg)
    n_latency = Histogram("lineage_neo4j_query_ms","Neo4j query latency (ms)", ["kind"],
                          buckets=(1,5,10,20,50,100,200,500,1000,2000,5000), registry=reg)
    t_latency = Histogram("lineage_tg_query_ms","TigerGraph query latency (ms)", ["kind"],
                          buckets=(1,5,10,20,50,100,200,500,1000,2000,5000), registry=reg)
    reachable_hist = Histogram("lineage_reachable_nodes","Reachable nodes per source", ["backend"],
                               buckets=(0,1,2,5,10,20,50,100,200,500,1000), registry=reg)
    path_count_g = Gauge("lineage_path_count","Bounded path count to sinks", ["backend"], registry=reg)
    runtime_hist = Histogram("lineage_compare_runtime_ms","Comparator wall time (ms)",
                             buckets=(100,200,500,1000,2000,5000,10000,30000,60000), registry=reg)
    pub_success = Counter("lineage_publish_success_total","Successful artifact publishes", ["backend"], registry=reg)
    pub_failure = Counter("lineage_publish_failure_total","Failed artifact publishes", ["backend"], registry=reg)

    t_run0=time.monotonic()

    # Select sources
    n_driver = GraphDatabase.driver(n_uri, auth=(n_user,n_pwd))
    with n_driver.session() as s:
        sources = s.run("MATCH (s:Node)-[:FLOWS_TO]->() RETURN DISTINCT s.id AS id LIMIT $L", L=src_limit).value()

    rows: List[dict] = []
    diff_count=0
    for sid in sources:
        t0=time.monotonic(); n_edges = neo4j_get_edges(n_driver, sid, max_depth); n_latency.labels("edges").observe((time.monotonic()-t0)*1000)
        t0=time.monotonic(); n_sinks = neo4j_sinks(n_driver, sid); n_latency.labels("sinks").observe((time.monotonic()-t0)*1000)

        t0=time.monotonic(); t_edges = tg_get_edges(t_host, t_graph, t_user, t_pwd, sid, max_depth); t_latency.labels("edges").observe((time.monotonic()-t0)*1000)
        t0=time.monotonic(); t_sinks = tg_sinks(t_host, t_graph, t_user, t_pwd, sid); t_latency.labels("sinks").observe((time.monotonic()-t0)*1000)

        n_reach, _, n_maxh = bfs_metrics(n_edges, sid, max_depth)
        t_reach, _, t_maxh = bfs_metrics(t_edges, sid, max_depth)

        n_paths = bounded_path_count(n_edges, sid, n_sinks, max_depth)
        t_paths = bounded_path_count(t_edges, sid, t_sinks, max_depth)

        reachable_hist.labels("neo4j").observe(len(n_reach))
        reachable_hist.labels("tigergraph").observe(len(t_reach))
        if n_paths is not None: path_count_g.labels("neo4j").set(float(n_paths))
        if t_paths is not None: path_count_g.labels("tigergraph").set(float(t_paths))

        row = {
            "source": sid,
            "eq_sinks": n_sinks == t_sinks,
            "eq_reachable": n_reach == t_reach,
            "eq_max_depth": n_maxh == t_maxh,
            "eq_path_count": (n_paths == t_paths) or (n_paths is None and t_paths is None),
            "neo4j_sinks": len(n_sinks), "tigergraph_sinks": len(t_sinks),
            "neo4j_reachable": len(n_reach), "tigergraph_reachable": len(t_reach),
            "neo4j_max_depth": n_maxh, "tigergraph_max_depth": t_maxh,
            "neo4j_path_count": -1 if n_paths is None else int(n_paths),
            "tigergraph_path_count": -1 if t_paths is None else int(t_paths),
        }
        if not (row["eq_sinks"] and row["eq_reachable"] and row["eq_max_depth"] and row["eq_path_count"]):
            diff_count += 1
        rows.append(row)

    df = pd.DataFrame(rows)
    csv_path = os.path.join(outdir,"comparison.csv")
    md_path  = os.path.join(outdir,"comparison.md")
    html_path= os.path.join(outdir,"index.html")
    df.to_csv(csv_path, index=False)
    with open(md_path, "w") as f:
        f.write(f"# Parity Summary Sources: **{len(df)}** — Differences: **{diff_count}**")
    render_html(df, html_path, max_depth=max_depth, sources_limit=src_limit)

    # Metrics push
    runs.inc(); sources_g.set(len(df)); mismatches.inc(diff_count)
    runtime_hist.observe((time.monotonic()-t_run0)*1000)
    if pg:
        push_to_gateway(pg, job="lineage_compare", registry=reg)

    # Optional publish
    publisher = None
    try:
        publisher = build_publisher_from_env()
    except PublishError as e:
        print(f"[WARN] Publishing disabled due to configuration error: {e}", file=sys.stderr)

    if publisher:
        ts = datetime.datetime.utcnow().strftime("%Y/%m/%d/%H%M%SZ")
        prefix = os.getenv("PUBLISH_PREFIX","lineage/reports").strip("/")
        base_key = f"{prefix}/{ts}"
        artifacts = [
            Artifact(local_path=csv_path, remote_key=f"{base_key}/comparison.csv"),
            Artifact(local_path=md_path,  remote_key=f"{base_key}/comparison.md"),
            Artifact(local_path=html_path,remote_key=f"{base_key}/index.html",),
        ]
        diffs_json = os.path.join(outdir,"diffs.json")
        if os.path.exists(diffs_json):
            artifacts.append(Artifact(local_path=diffs_json, remote_key=f"{base_key}/diffs.json"))
        try:
            results = publisher.publish(artifacts)
            for _ in results:
                pub_success.labels(os.getenv("PUBLISH_BACKEND","s3")).inc()
            manifest = {
                "generated_at": datetime.datetime.utcnow().isoformat()+"Z",
                "count_sources": len(df),
                "differences": diff_count,
                "artifacts": [{"bucket_or_project":"", "key": k} for _,k in results]
            }
            with open(os.path.join(outdir,"manifest.json"),"w") as mf:
                json.dump(manifest, mf, indent=2)
        except Exception as e:
            print(f"[ERROR] Publish failed: {e}", file=sys.stderr)
            pub_failure.labels(os.getenv("PUBLISH_BACKEND","s3")).inc()
            if pg:
                push_to_gateway(pg, job="lineage_compare", registry=reg)

    print(f"Wrote reports to {outdir}. Publishing={'enabled' if publisher else 'disabled'}.")

if __name__ == "__main__":
    main()

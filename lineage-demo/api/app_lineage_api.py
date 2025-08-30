import os
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "password")

app = FastAPI(title="Lineage API (Cytoscape-friendly)")

# Allow browser access during local dev; tighten for prod as needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

def _node_id(n):
    return (n.get("site_key") or n.get("server_key") or n.get("soft_key")
            or n.get("dir_key") or n.get("feed_key") or n.get("pde_key")
            or n.get("dc_id") or n.get("rack_id"))

def to_node_payload(n):
    lbls = list(n.labels)
    # Prefer the specific label if :Asset also present
    label = next((l for l in lbls if l != "Asset"), lbls[0] if lbls else "Node")
    return {"data": {"id": _node_id(n), "label": label, "type": label, **dict(n)}}

def to_edge_payload(r, src, dst):
    return {"data": {
        "id": f"{src}-{r.type}-{dst}",
        "source": src, "target": dst,
        "label": r.type, **dict(r)
    }}

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/lineage")
def lineage(
    pde_key: str = Query(default=None),
    site_key: str = Query(default=None),
    max_hops: int = Query(4, ge=1, le=8)
):
    if not pde_key and not site_key:
        return {"error": "Provide either pde_key or site_key"}

    start_match = ("MATCH (start:PDE {pde_key:$key})" if pde_key
                   else "MATCH (start:Website {site_key:$key})")
    # Structural + flow relationships
    rels = "<HAS|<EXPOSES|<USES|<RUNS|<HOSTED_ON|FLOWS_TO"

    cypher = f"""
    {start_match}
    CALL apoc.path.expandConfig(start, {{
      relationshipFilter: "{rels}",
      minLevel: 1, maxLevel: $max_hops, bfs:true
    }}) YIELD path
    WITH collect(path) AS paths
    WITH [p IN paths | nodes(p)] AS node_lists, [p IN paths | relationships(p)] AS rel_lists
    WITH apoc.coll.toSet(apoc.coll.flatten(node_lists)) AS uniq_nodes,
         apoc.coll.toSet(apoc.coll.flatten(rel_lists)) AS uniq_rels
    UNWIND uniq_nodes AS n
    WITH collect(n) AS nodes, uniq_rels
    UNWIND uniq_rels AS r
    WITH nodes, r, startNode(r) AS s, endNode(r) AS e
    RETURN nodes AS nodes, collect(DISTINCT [r,s,e]) AS edges
    """
    key = pde_key or site_key
    with driver.session() as s:
        rec = s.run(cypher, key=key, max_hops=max_hops).single()
        nodes = rec["nodes"] if rec else []
        edges = rec["edges"] if rec else []

    node_payloads = {}
    for n in nodes:
        payload = to_node_payload(n)
        node_payloads[payload["data"]["id"]] = payload

    edge_payloads = []
    for r, s, e in edges:
        src = _node_id(s)
        dst = _node_id(e)
        if src and dst:
            edge_payloads.append(to_edge_payload(r, src, dst))

    return {"nodes": list(node_payloads.values()), "edges": edge_payloads}

import socket
from urllib.parse import urlparse
from fastapi.responses import JSONResponse

print(f"[Lineage API] NEO4J_URI={NEO4J_URI}  NEO4J_USER={NEO4J_USER}")

@app.get("/debug/neo4j")
def debug_neo4j():
    info = {"uri": NEO4J_URI, "user": NEO4J_USER}
    try:
        u = urlparse(NEO4J_URI)
        host = u.hostname
        port = u.port or 7687
        info["parsed_host"] = host
        info["parsed_port"] = port
        try:
            addrs = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
            info["dns"] = [a[4][0] for a in addrs]
        except Exception as e:
            info["dns_error"] = str(e)
        try:
            with driver.session() as s:
                val = s.run("RETURN 1 AS one").single().get("one")
            info["driver_test"] = f"ok (RETURN {val})"
        except Exception as e:
            info["driver_error"] = str(e)
    except Exception as e:
        info["error"] = str(e)
    return JSONResponse(info)

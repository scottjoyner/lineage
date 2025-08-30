# Lineage Compare — End‑to‑End Bake‑off (Neo4j vs TigerGraph)

This repo builds two **containerized lineage systems** side‑by‑side using the **same normalized inputs** so you can compare **end‑to‑end lineage** (sources → sinks) between **Neo4j** and **TigerGraph**.

## Why this exists
You have repo/site scanners that emit lineage hints (e.g., file → dataset, table → dashboard). This addendum provides:
- A **normalizer** that converts heterogeneous scanner outputs to a **graph‑agnostic** schema.
- **Neo4j** and **TigerGraph** **loaders** that ingest the same normalized graph.
- A **comparator** that runs equivalent **reachability to sinks** queries and reports differences.

## TL;DR
```bash
cp .env.example .env   # edit passwords as needed
make normalize         # data/input -> data/normalized
make up-neo4j          # start Neo4j & load
make up-tg             # start TG, bootstrap schema, load
make compare           # write data/output/comparison.{csv,md}
```

## Inputs
Place your scanner outputs under `data/input/` (JSON). Minimal record example:
```json
{"source": "repoA/a.py", "target": "s3://bucket/raw/events.json", "op": "write", "repo": "repoA"}
```

## Normalized schema
- **Node**: `{id, name, type, repo}`
- **Edge**: `(src)-[:FLOWS_TO {op, ts, weight}]->(dst)`
IDs are **stable SHA‑1 hashes** of `name` to ensure joins across backends.

## Backends
- **Neo4j**: merged `Node`/`FLOWS_TO`, uniqueness constraint on `Node(id)`, variable‑length path query for sinks.
- **TigerGraph**: `Node` vertex + `FLOWS_TO` edge; installed GSQL query `getSinks` traverses until convergence and returns sinks.

## Security/ops best practices
- Containers run as non‑root and use minimal Python base images.
- Healthchecks for services; loaders wait on availability.
- Secrets via `.env` (keep out of VCS). Read‑only mounts where possible.
- Clear separation of concerns: normalize → load → compare.

## Files to know
- `scripts/normalize.sh` — runs the normalizer in a throwaway Python container.
- `scripts/tg_bootstrap.sh` — creates TigerGraph schema and installs `getSinks` (one‑time per volume).
- `loaders/neo4j/loader.py` — upserts nodes/edges into Neo4j via Bolt.
- `loaders/tigergraph/loader.py` — upserts vertices/edges via pyTigerGraph.
- `compare/compare_lineage.py` — executes sinks queries on both backends and reports diffs.

## Extending
- Add scanner adapters inside `scripts/normalize.sh`'s embedded Python (or replace with your own normalizer).
- Add more queries for parity (e.g., path counts, max depth, degree distributions).

## Cleanup
```bash
make down
make clean
```

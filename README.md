# Lineage Demo (Neo4j + FastAPI for Cytoscape.js)

This repo spins up Neo4j (with APOC) and a small FastAPI service that returns Cytoscape-friendly elements for lineage graphs across layers:

Website → Server → Software → Directory → Feed → PDE (+ time-aware flow facts and FlowRun scan events).

## Prereqs
- Docker & Docker Compose

## Quickstart

1) Copy `.env.example` → `.env` and set a strong password:
```bash
cp .env.example .env
# edit .env, set NEO4J_PASSWORD
```

2) Start services:
```bash
docker compose --env-file .env up -d --build
```

3) Load constraints & seed data (once Neo4j is up):
```bash
# Create constraints
docker exec -it lineage_neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -f /import/01_constraints.cypher

# Import seed CSVs
docker exec -it lineage_neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" -f /import/02_import.cypher
```

4) Try the API:
- Swagger UI: http://localhost:${API_PORT:-8000}/docs
- Example lineage for a PDE (downstream/upstream mixed within 4 hops):
```
GET http://localhost:${API_PORT:-8000}/lineage?pde_key=dw.orders_fact.amount_usd&max_hops=4
```
- Example lineage for a Website:
```
GET http://localhost:${API_PORT:-8000}/lineage?site_key=orders.example.com&max_hops=4
```

These endpoints return `{ nodes: [...], edges: [...] }` ready for Cytoscape.js:
```js
cy.add(result.nodes);
cy.add(result.edges);
```

## Data Model Notes

- Structural edges (HOSTED_ON, RUNS, USES, EXPOSES, HAS) are stable, enabling fast queries.
- Flow edges (READS, WRITES, FLOWS_TO) carry time- and job-scoped facts (`job_id`, `ts`, `rows`, `bytes`).
- `FlowRun` captures a *global* scanning session (e.g., repo/corpus scan that inferred lineage). We record which feeds were READ_FROM/WRITE_TO by the scan to stitch global context.

## Files

- `docker-compose.yml` — Neo4j + API stack
- `api/` — FastAPI service (`/lineage`, `/healthz`)
- `neo4j/import/*.csv` — seed nodes and relationships
- `neo4j/import/01_constraints.cypher` — indexes & constraints
- `neo4j/import/02_import.cypher` — CSV importer

## Security & Prod Tips
- Always set a strong `NEO4J_PASSWORD` in `.env` and keep `.env` out of source control.
- Lock CORS and network exposure in `api/app_lineage_api.py` for production.
- Consider periodic rollups for extremely chatty flow edges; keep only latest facts on edges and archive historical events in separate nodes.
- For large scans: stream writes with idempotent keys (`MERGE` on deterministic keys), batch with `USING PERIODIC COMMIT`, use APOC to pre-aggregate before persisting.

## Cytoscape Styling Ideas
Use `data.type` to color nodes by layer and `data.op` on edges to style different lineage ops (copy/mask/transform).


## Web UI (Cytoscape.js)
A minimal static UI (Nginx) is included.

- URL: `http://localhost:${WEB_PORT:-3000}`
- It fetches from the API (defaults to `http://localhost:${API_PORT:-8000}`). You can override via query param `?api=...` or the input field in the left panel.

Controls:
- Choose PDE or Website, enter the key, set max hops, then **Load graph**
- Change layout (cose / breadthfirst / concentric / grid)
- Click nodes/edges to see properties


## Five demo views (web/)
- `/index.html` – Explorer (cose / breadthfirst / concentric / grid, inspect, export)
- `/view-dagre.html` – Hierarchical DAG with **cytoscape-dagre**
- `/view-cose-bilkent.html` – Large graph layout with **cytoscape-cose-bilkent**
- `/view-sbgn.html` – SBGN stylesheet demo (via **cytoscape-sbgn-stylesheet**)
- `/view-grid.html` – Table-like, multi-column level view (preset positions by layer)

Tip: Pass `?api=http://host:8000` to any page to point at a remote API.

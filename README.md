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


## Local Neo4j passthrough (dev)
If you want the API to use a **local Neo4j** running on your host instead of the container:

**Option A — Compose override (recommended)**
1) Create or use the included `docker-compose.local.yml`.
2) Ensure your local Neo4j is listening on Bolt (default `localhost:7687`).
3) Start API + Web only (Neo4j container is put under the `db` profile and won’t start by default):
```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml --env-file .env up -d --build
```
4) If needed, set `NEO4J_URI` in `.env` (or export it) to `bolt://host.docker.internal:7687`.
   - On Linux, this override maps `host.docker.internal` to the host gateway.

**Option B — Env-only**
1) Keep using the default `docker-compose.yml` but set:
   - `NEO4J_URI=bolt://host.docker.internal:7687`
   - `NEO4J_USER=neo4j`
   - `NEO4J_PASS=<your local password>` (if different from `NEO4J_PASSWORD`)
2) Bring up the stack (Neo4j container will still start; you can avoid it by using Option A).

**Start the containerized Neo4j only when you want it**
```bash
# add the db profile to include the containerized Neo4j
docker compose -f docker-compose.yml -f docker-compose.local.yml --env-file .env --profile db up -d --build
```

**Notes**
- The API resolves `host.docker.internal` on Linux via `extra_hosts: host-gateway` in the override.
- You can also run the API outside Docker entirely; just set `NEO4J_URI=bolt://localhost:7687` and
  `NEO4J_USER` / `NEO4J_PASS` in your shell.


### Compose v2 notes
- Removed deprecated `version` key from compose files.
- `docker-compose.local.yml` now overrides `api.depends_on` to `[]` so you can run without the Neo4j container (use `--profile db` to include it).


## One-shot scan → ingest (elegant option)

Keep API/UI separate for fast reads, and run ingestion as a **one-shot job** when you need to refresh lineage.

**Setup**
- Set `SCAN_PATH` in `.env` to a repo directory (default: `./sample-repo` included).
- Optionally set `SCANNER_CONN` to a connection name (see `scanner/connections.yaml`).

**Run with containerized Neo4j (compose DB)**
```bash
# if you want the db, api, web and the one-shot ingest
docker compose --env-file .env --profile db --profile ingest up -d --build
# or just run the ingest job once:
docker compose --env-file .env --profile ingest up --build ingest
```

**Run with local Neo4j (passthrough)**
```bash
# point at your host DB
echo "NEO4J_URI=bolt://host.docker.internal:7687" >> .env
echo "NEO4J_PASS=<your-local-password>" >> .env

docker compose -f docker-compose.yml -f docker-compose.local.yml --env-file .env --profile ingest up --build ingest
```

The `ingest` service:
- mounts your repo at `/scan`
- runs `scanner ingest` (scan → push via Neo4j driver)
- exits upon completion (idempotent MERGEs in Cypher)


## Event-driven publisher/subscriber queue (SQLite)

New services:
- `queue` (FastAPI, port `${QUEUE_API_PORT:-9000}`): publish events, enqueue jobs, SSE stream, GitHub webhook (`/hook/github`), backup endpoint (`/admin/backup`).
- `worker` (Python): polls the SQLite queue and runs `scanner ingest` in-process (supports local repo paths or `git_url` checkout).

**Persistence**: SQLite is stored under `./queue/data` (volume). **Backups**: `GET /admin/backup` writes a copy under `./queue/backups` (volume).

**Run**
```bash
docker compose --env-file .env up -d --build queue worker
# UI at http://localhost:${WEB_PORT:-3000}/queue/index.html
```

**Enqueue a job**
```bash
curl -X POST http://localhost:${QUEUE_API_PORT:-9000}/jobs/ingest \      -H 'Content-Type: application/json' \      -d '{"repo_path":"./sample-repo","conn_name":"demo_pg"}'
```

**Stream events (SSE)**
```bash
curl -N http://localhost:${QUEUE_API_PORT:-9000}/events/stream
```

**GitHub webhook**
- Point your repo webhook to `POST http://<host>:${QUEUE_API_PORT:-9000}/hook/github` (content type: `application/json`).
- Optional: set `GITHUB_WEBHOOK_SECRET` in `.env` to enable signature verification.
- On push events, an ingest job is queued with `git_url` & branch from the payload.

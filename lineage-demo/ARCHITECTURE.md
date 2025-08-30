# Lineage Platform — Architecture & Operations

## Components
- **Neo4j**: Graph DB that stores assets, structure, and flows (PDE-level).
- **API (FastAPI)**: Read-only service that queries Neo4j and emits Cytoscape-ready graphs.
- **Web (Nginx + static)**: Five demo views with Cytoscape layouts (cose, dagre, cose-bilkent, SBGN, grid).
- **Scanner**: Pluggable static analysis for SQL/dbt/Airflow producing feeds/PDEs/flows.
- **Queue (FastAPI + SQLite)**: Publish events, enqueue ingestion jobs, SSE stream, webhook endpoint, backups, Prometheus `/metrics`.
- **Worker (Python)**: N-concurrency consumer with retries & exponential backoff, Prometheus metrics.

## Scale-out strategy
- **Workers**: Increase `WORKER_CONCURRENCY` per pod; scale replicas horizontally. SQLite supports multiple readers + one writer. For very high throughput, migrate to Postgres or a managed queue (Pub/Sub/SQS/Kafka). The queue is abstracted via SQL — a lightweight adapter can target Postgres with minimal changes.
- **API**: Stateless; scale horizontally behind a load balancer; caches can be added (e.g., Redis) on expensive queries.
- **Neo4j**: Use Aura or cluster; enable page cache sizing, tune Bolt pool; index high-traffic properties (provided in `01_constraints.cypher`).

## Reliability
- **Retries with backoff**: Worker requeues failures up to `max_attempts`, with jittered exponential backoff.
- **Dead-letter**: Jobs that exceed attempts go to `status=error`; requeue via API is trivial to add.
- **Backups**: `/admin/backup` snapshots SQLite to `/queue/backups`. Schedule rsync/S3 sync for durability.
- **Idempotency**: MERGE-based writes prevent duplicates; `lineage_hash` on edges dedups PDE flows.

## Observability
- **Metrics**: API exposes `/metrics` (queue counters/gauges). Worker serves Prometheus on `:9100`.
- **Logs**: Structured JSON logs across services with `python-json-logger`. Include job ids and durations.
- **Tracing**: Optionally add OpenTelemetry `OTEL_EXPORTER_OTLP_ENDPOINT` to API/Worker later.

## Security
- **Secrets**: Use env vars / secrets manager; never bake into images.
- **CORS**: Open for local dev; restrict origins in prod.
- **Webhooks**: HMAC signature validation for GitHub; rate limit `/publish` in WAF if exposed.

## Kubernetes blueprint (prod)
- Deploy each service as a Deployment; expose API/Queue behind Ingress.
- Use a **PersistentVolume** for SQLite (ReadWriteOnce). For higher scale, swap SQLite for Postgres.
- HPA on Worker by queue depth metric; PodDisruptionBudgets for API/Worker.
- CronJob for `/admin/backup` + off-cluster snapshot upload.

## Testing
- Unit tests for backoff & API CRUD.
- Integration tests to run worker against a temp DB and mock scanner (future work).
- Contract tests for the lineage API response format to keep Cytoscape clients stable.

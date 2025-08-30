# Decision Memo: Neo4j vs TigerGraph for Graph Workloads
**Owner:** Scott Joyner (AssistX) • **Date:** 2025‑08‑30 • **Audience:** Exec & Eng Leads • **Purpose:** Choose the right engine per workload stage.

---

## Executive Decision (TL;DR)
- **Primary system of record & product queries:** **Neo4j** (fast iteration, rich Cypher, strong consistency via leader, excellent tooling).  
- **Heavy, multi-hop analytics on very large graphs (parallel throughput):** **TigerGraph** (compiled GSQL, partitioned execution, cluster scale‑out).  
- We will **use both where each is strongest**. Neo4j remains our app backbone; **escalate** to TigerGraph for analytics that exceed single‑box memory or require consistent cluster‑parallel runtimes.

---

## Workload Profile (fill in → drives the choice)
| Metric | Current | Target / SLA | How to Measure |
|---|---:|---:|---|
| Nodes | [n_total] | — | `MATCH (n) RETURN count(n)` |
| Edges | [e_total] | — | `MATCH ()-[r]->() RETURN count(r)` |
| Peak write QPS | [w_qps_peak] | [SLA] | App metrics / driver logs |
| Read QPS | [r_qps] | [SLA] | App metrics / driver logs |
| 95p read latency | [p95_read_ms] | [SLA] | APM / driver metrics |
| Typical query depth | [hops] | [hops] | Query review |
| GDS projection size | [bytes_est] | fit in RAM+30% | `gds.graph.project.estimate(...)` |
| Analytics cadence | [per day/week] | [SLA] | Job scheduler |
| Ops appetite | [low/med/high] | — | Team consensus |

> **Decision rule of thumb:** If the **GDS projected working set fits** comfortably in memory on a dedicated analytics box and **write throughput** is well within a single leader’s capacity, stay on **Neo4j**. If analytics **do not fit in memory** or repeatedly require deeper, parallel multi‑hop traversals over a single global graph, run them on **TigerGraph**.

---

## Option A — Neo4j (Recommended for OLTP + targeted analytics)
**Why here:** Best-in-class developer velocity, ad‑hoc Cypher, strong ecosystem; read scaling via replicas; **GDS** for algorithms when projections fit memory.

**Strengths**
- Intuitive **Cypher** for product features and rapid iteration.
- **Leader‑coordinated writes** (strong consistency); reads scale out.
- **GDS**: large algorithm catalog on **in‑memory projections**.

**Risks / Limits**
- **Write throughput** bounded by a **single leader per database**.
- **Analytics must fit memory** (projection sizing & heap planning).
- Cross‑DB analytics via Composite/Fabric adds operational complexity.

**How we use it now / near‑term**
- Keep **app state & serving** in Neo4j.
- Use **GDS** for sampling, PageRank/centrality, community, similarity on **scoped projections**.
- Federate sources with **Composite DBs** if we need logical separation.

**Acceptance tests (pass → keep on Neo4j)**
- GDS memory estimate ≤ **~60–70%** of available RAM on the analytics host.
- p95 latency for core product queries meets SLA under **peak reads**.
- Sustained writes do not saturate the leader or degrade latency materially.

---

## Option B — TigerGraph (Recommended for large, parallel analytics)
**Why here:** **Compiled** GSQL + **accumulators** executing **in parallel** across partitions; excels on **very large graphs** and **deep multi‑hop** analytics.

**Strengths**
- **MPP** scale‑out; parallel execution across graph partitions.
- **GSQL accumulators** model per‑vertex/global state for analytics.
- REST++/installed queries expose analytics as services.

**Risks / Costs**
- Higher **ops complexity** (cluster sizing, partitioning, queues).
- **Language shift** (Cypher → GSQL patterns with ACCUM/POST‑ACCUM).
- **Community Edition** is **single‑server** with size limits (fine for dev).

**When to escalate**
- Analytics **don’t fit in memory** or **routinely exceed** Neo4j GDS runtime targets.
- You need **minutes‑scale** consistency on **global, multi‑TB graphs**.
- You want to **isolate analytics** from the OLTP plane.

**Acceptance tests (pass → move a workload to TigerGraph)**
- End‑to‑end analytics job meets SLA with **predictable runtimes** as data grows.
- Cluster remains within resource budgets at projected 12‑month growth.
- Operational playbooks (backups, upgrades, on‑call) validated.

---

## Migration Notes (only if/when needed)
- **Model:** Neo4j labels/rel‑types → **vertex/edge types**; relationship props → **edge attributes**.
- **ETL:** APOC/CSV export → **Loading Jobs** in TigerGraph.
- **Queries:** Cypher → **GSQL** with accumulators (SELECT…ACCUM…POST‑ACCUM).
- **Clients:** Bolt → **pyTigerGraph/REST++** (use POST for large payloads).

---

## Next 2‑Week Plan (evidence‑driven)
1) **Measure**: Fill the table above; run `gds.graph.project.estimate` on top‑3 analytics use cases.  
2) **Benchmark**: Neo4j soak test for peak **read+write**; capture p95 latency & leader CPU.  
3) **Prototype analytics**:  
   - 3a) **Neo4j GDS** on scoped projection(s); record memory and wall‑clock.  
   - 3b) **TigerGraph PoC** (small cluster): port one deep‑hop algorithm; record runtime curve vs data size.  
4) **Decide per‑workload**: Keep on Neo4j vs move to TigerGraph; document SLOs & cost.  
5) **Operationalize**: If TG wins a workload, write runbooks (backup, scaling, incident response).

---

## Appendix — Quick commands you’ll need
- **Counts**
  - Nodes: `MATCH (n) RETURN count(n);`
  - Rels: `MATCH ()-[r]->() RETURN count(r);`
- **By label/type**
  - `MATCH (n:Label) RETURN count(n);`
  - `MATCH ()-[r:TYPE]->() RETURN count(r);`
- **Estimate GDS memory**
  ```cypher
  CALL gds.graph.project.estimate(
    'proj',
    {Person: {}},                // <replace with your labels>
    {KNOWS: {orientation: 'NATURAL'}} // <replace with your rels>
  )
  YIELD requiredMemory, nodeCount, relationshipCount, bytesMin, bytesMax;
  ```

---

## Recommendation Snapshot
**Default:** Build and serve in **Neo4j**.  
**Escalate analytics to TigerGraph** only when the evidence (memory fit, runtime, or write pressure) shows clear benefit.  
This keeps engineering velocity high while unlocking parallel analytics **when—not before—it’s needed**.

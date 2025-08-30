// Nodes
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_websites.csv' AS row
MERGE (n:Website:Asset {site_key: row.site_key})
  ON CREATE SET n.url=row.url, n.env=row.env, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_servers.csv' AS row
MERGE (n:Server:Asset {server_key: row.server_key})
  ON CREATE SET n.fqdn=row.fqdn, n.ip=row.ip, n.os=row.os, n.rack_id=row.rack_id, n.dc_id=row.dc_id, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_software.csv' AS row
MERGE (n:Software:Asset {soft_key: row.soft_key})
  ON CREATE SET n.name=row.name, n.version=row.version, n.kind=row.kind, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_directories.csv' AS row
MERGE (n:Directory:Asset {dir_key: row.dir_key})
  ON CREATE SET n.path=row.path, n.fs=row.fs, n.mount=row.mount, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_feeds.csv' AS row
MERGE (n:Feed:Asset {feed_key: row.feed_key})
  ON CREATE SET n.name=row.name, n.feed_type=row.feed_type, n.format=row.format, n.system=row.system, n.schema_hash=row.schema_hash, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_pdes.csv' AS row
MERGE (n:PDE:Asset {pde_key: row.pde_key})
  ON CREATE SET n.name=row.name, n.data_type=row.data_type, n.pii=toBoolean(row.pii), n.domain=row.domain, n.owner=row.owner, n.tags=apoc.convert.fromJsonList(row.tags);

// Relationships (structural)
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_website_hosted_on_server.csv' AS row
MATCH (a:Website {site_key:row.site_key})
MATCH (b:Server  {server_key:row.server_key})
MERGE (a)-[:HOSTED_ON]->(b);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_server_runs_software.csv' AS row
MATCH (a:Server {server_key:row.server_key})
MATCH (b:Software {soft_key:row.soft_key})
MERGE (a)-[:RUNS]->(b);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_software_uses_directory.csv' AS row
MATCH (a:Software {soft_key:row.soft_key})
MATCH (b:Directory {dir_key:row.dir_key})
MERGE (a)-[:USES]->(b);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_directory_exposes_feed.csv' AS row
MATCH (a:Directory {dir_key:row.dir_key})
MATCH (b:Feed {feed_key:row.feed_key})
MERGE (a)-[:EXPOSES]->(b);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_feed_has_pde.csv' AS row
MATCH (a:Feed {feed_key:row.feed_key})
MATCH (b:PDE  {pde_key:row.pde_key})
MERGE (a)-[:HAS]->(b);

// Flow / event nodes
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///nodes_flow_runs.csv' AS row
MERGE (e:FlowRun {job_id: row.job_id})
  ON CREATE SET e.ts=row.ts, e.pipeline=row.pipeline, e.status=row.status;

// Flow edges with run facts
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_software_reads.csv' AS row
MATCH (s:Software {soft_key:row.soft_key})
MATCH (f:Feed {feed_key:row.feed_key})
MERGE (s)-[r:READS {job_id: row.job_id}]->(f)
  ON CREATE SET r.ts=row.ts, r.bytes=toInteger(row.bytes), r.rows=toInteger(row.rows);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_software_writes.csv' AS row
MATCH (s:Software {soft_key:row.soft_key})
MATCH (f:Feed {feed_key:row.feed_key})
MERGE (s)-[w:WRITES {job_id: row.job_id}]->(f)
  ON CREATE SET w.ts=row.ts, w.bytes=toInteger(row.bytes), w.rows=toInteger(row.rows);

// PDE->PDE lineage edges
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_pde_flows_to.csv' AS row
MATCH (a:PDE {pde_key:row.src_pde_key})
MATCH (b:PDE {pde_key:row.tgt_pde_key})
MERGE (a)-[fl:FLOWS_TO {job_id: row.job_id}]->(b)
  ON CREATE SET fl.ts=row.ts, fl.op=row.op, fl.lineage_hash=row.lineage_hash,
                fl.quality=row.quality, fl.bytes=toInteger(row.bytes), fl.rows=toInteger(row.rows);

// FlowRun ~ feed backrefs (scan discovered these relations)
USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_flowrun_read_from.csv' AS row
MATCH (e:FlowRun {job_id: row.job_id})
MATCH (f:Feed {feed_key: row.feed_key})
MERGE (e)-[:READ_FROM]->(f);

USING PERIODIC COMMIT 1000
LOAD CSV WITH HEADERS FROM 'file:///rel_flowrun_write_to.csv' AS row
MATCH (e:FlowRun {job_id: row.job_id})
MATCH (f:Feed {feed_key: row.feed_key})
MERGE (e)-[:WRITE_TO]->(f);

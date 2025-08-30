from neo4j import GraphDatabase
from typing import Dict, List, Tuple
from .models import PDE, Feed

def ingest(driver, feeds: Dict[str, Feed], pdes: Dict[str, PDE], flows: List[Tuple[str,str]], job_id: str, ts: str):
    cypher = '''
    MERGE (fr:FlowRun {job_id:$job_id})
      ON CREATE SET fr.ts=$ts, fr.pipeline='repo-scan', fr.status='DONE';

    UNWIND $feeds AS f
    MERGE (n:Feed:Asset {feed_key:f.key})
      ON CREATE SET n.name=f.name, n.feed_type=f.feed_type, n.format=f.format, n.system=f.system, n.schema_hash=coalesce(f.schema_hash,''), n.owner=coalesce(f.owner,'');

    UNWIND $pdes AS p
    MERGE (n:PDE:Asset {pde_key:p.key})
      ON CREATE SET n.name=p.name, n.data_type=coalesce(p.data_type,'string'), n.pii=coalesce(p.pii,false), n.domain=coalesce(p.domain,''), n.owner=coalesce(p.owner,'');

    UNWIND $has AS rel
    MATCH (f:Feed {feed_key:rel.feed})
    MATCH (p:PDE {pde_key:rel.pde})
    MERGE (f)-[:HAS]->(p);

    UNWIND $flows AS fl
    MATCH (a:PDE {pde_key:fl[0]})
    MATCH (b:PDE {pde_key:fl[1]})
    MERGE (a)-[r:FLOWS_TO {job_id:$job_id}]->(b)
      ON CREATE SET r.ts=$ts, r.op='transform', r.lineage_hash=substring(apoc.util.sha256(fl[0] + '->' + fl[1]),0,12), r.quality='PASS';
    '''
    feed_list = [f.__dict__ for f in feeds.values()]
    pde_list = [p.__dict__ for p in pdes.values()]
    has_list = []
    # simple heuristic: feed.name == table part of pde.key (table.col)
    for f in feeds.values():
        prefix = f.name + "."
        for p in pdes.values():
            if p.key.startswith(prefix):
                has_list.append({"feed": f.key, "pde": p.key})
    with driver.session() as s:
        s.run(cypher, job_id=job_id, ts=ts, feeds=feed_list, pdes=pde_list, has=has_list, flows=flows).consume()

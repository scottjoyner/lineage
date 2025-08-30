import os, csv, hashlib
from typing import Dict, List, Tuple
from ..models import PDE, Feed

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def emit(out_dir: str,
         feeds: Dict[str, Feed],
         pdes: Dict[str, PDE],
         flows: List[Tuple[str,str]],
         soft_key: str,
         job_id: str,
         ts: str):
    ensure_dir(out_dir)

    with open(os.path.join(out_dir, "nodes_feeds.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["feed_key","name","feed_type","format","system","schema_hash","owner","tags"])
        for k, fd in feeds.items():
            w.writerow([fd.key, fd.name, fd.feed_type, fd.format, fd.system, fd.schema_hash, fd.owner, "[]"])

    with open(os.path.join(out_dir, "nodes_pdes.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pde_key","name","data_type","pii","domain","owner","tags"])
        for k, p in pdes.items():
            w.writerow([p.key, p.name, p.data_type, str(p.pii).lower(), p.domain, p.owner, "[]"])

    with open(os.path.join(out_dir, "rel_pde_flows_to.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["src_pde_key","tgt_pde_key","job_id","ts","op","lineage_hash","quality","bytes","rows"])
        for a, b in flows:
            w.writerow([a, b, job_id, ts, "transform", hashlib.sha256(f"{a}->{b}".encode()).hexdigest()[:12], "PASS", 0, 0])

    with open(os.path.join(out_dir, "nodes_flow_runs.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["job_id","ts","pipeline","status"])
        w.writerow([job_id, ts, "repo-scan", "DONE"])

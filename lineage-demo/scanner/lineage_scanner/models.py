from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time, hashlib

@dataclass
class Connection:
    name: str
    system: str
    host: str = ""
    db: str = ""
    schema: str = ""
    extra: dict = field(default_factory=dict)

@dataclass
class PDE:
    key: str
    name: str
    data_type: str = "string"
    pii: bool = False
    domain: str = ""
    owner: str = ""
    tags: List[str] = field(default_factory=list)

@dataclass
class Feed:
    key: str
    name: str
    feed_type: str
    format: str = ""
    system: str = ""
    schema_hash: str = ""
    owner: str = ""
    tags: List[str] = field(default_factory=list)

@dataclass
class FlowRun:
    job_id: str
    ts: str
    pipeline: str = "repo-scan"
    status: str = "DONE"

@dataclass
class EdgePDEFlow:
    src_pde_key: str
    tgt_pde_key: str
    job_id: str
    ts: str
    op: str = "transform"
    lineage_hash: str = ""
    quality: str = "PASS"
    bytes: int = 0
    rows: int = 0

@dataclass
class RWRecord:
    soft_key: str
    feed_key: str
    job_id: str
    ts: str
    bytes: int = 0
    rows: int = 0

def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def sha(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()[:12]

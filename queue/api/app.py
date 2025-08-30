import os, json, hmac, hashlib, datetime, asyncio, aiosqlite, logging, sqlite3
from typing import Optional
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from queue.logging_util import setup_json_logging

DB_PATH = os.getenv("DB_PATH", "/data/queue.db")
BACKUP_DIR = os.getenv("BACKUP_DIR", "/backups")
WEBHOOK_SECRET = os.getenv("GITHUB_WEBHOOK_SECRET", "")
LOG = setup_json_logging("queue.api")

app = FastAPI(title="Lineage Event Bus & Queue")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

INIT_SQL = '''
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  source TEXT NOT NULL,
  key TEXT NOT NULL,
  payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  scheduled_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  status TEXT NOT NULL,            -- queued|running|done|error|canceled
  type TEXT NOT NULL,              -- ingest
  priority INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  error TEXT,
  repo_path TEXT,
  git_url TEXT,
  git_branch TEXT,
  conn_name TEXT,
  owner TEXT,
  flow_job_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, scheduled_at);
'''

# Metrics
EVENTS_PUBLISHED = Counter("queue_events_published_total", "Events published")
JOBS_ENQUEUED = Counter("queue_jobs_enqueued_total", "Jobs enqueued", ["type"])
JOBS_STATUS = Gauge("queue_jobs_status", "Number of jobs by status", ["status"])
JOBS_CREATED_LAT = Histogram("queue_job_enqueue_latency_seconds", "Enqueue processing latency")

async def init_db():
    LOG.info({"event":"init_db", "db_path": DB_PATH})
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(INIT_SQL)
        await db.commit()

async def update_status_metrics():
    async with aiosqlite.connect(DB_PATH) as db:
        for st in ["queued","running","done","error","canceled"]:
            cur = await db.execute("SELECT COUNT(1) FROM jobs WHERE status=?", (st,))
            n = (await cur.fetchone())[0]
            JOBS_STATUS.labels(status=st).set(n)

@app.on_event("startup")
async def on_start():
    await init_db()
    await update_status_metrics()

@app.get("/healthz")
async def healthz():
    return {"ok": True, "db": DB_PATH}

@app.get("/metrics")
async def metrics():
    await update_status_metrics()
    data = generate_latest()  # default registry
    return JSONResponse(content=data.decode("utf-8"), media_type=CONTENT_TYPE_LATEST)

class PublishEvent(BaseModel):
    key: str = Field(..., description="event key, e.g. repo:push")
    payload: dict = Field(default_factory=dict)

@app.post("/publish")
async def publish(evt: PublishEvent):
    t0 = datetime.datetime.utcnow()
    now = t0.strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("INSERT INTO events(created_at, source, key, payload) VALUES (?,?,?,?)",
                         (now, "api", evt.key, json.dumps(evt.payload)))
        await db.commit()
    EVENTS_PUBLISHED.inc()
    await _notify_sse({"type":"event","key":evt.key,"payload":evt.payload,"created_at":now})
    JOBS_CREATED_LAT.observe((datetime.datetime.utcnow() - t0).total_seconds())
    LOG.info({"event":"publish","key":evt.key,"payload":evt.payload})
    return {"ok": True}

class IngestJob(BaseModel):
    repo_path: Optional[str] = None
    git_url: Optional[str] = None
    git_branch: Optional[str] = None
    conn_name: str
    owner: Optional[str] = ""
    priority: int = 0
    schedule_at: Optional[str] = None  # ISO; default now
    max_attempts: int = 3

@app.post("/jobs/ingest")
async def create_job(j: IngestJob):
    if not j.repo_path and not j.git_url:
        raise HTTPException(400, "Provide repo_path or git_url")
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    sched = j.schedule_at or now
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        cur = await db.execute(
            "INSERT INTO jobs(created_at, scheduled_at, status, type, priority, repo_path, git_url, git_branch, conn_name, owner, max_attempts) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (now, sched, "queued", "ingest", j.priority, j.repo_path, j.git_url, j.git_branch, j.conn_name, j.owner or "", j.max_attempts)
        )
        jid = cur.lastrowid
        await db.commit()
    JOBS_ENQUEUED.labels("ingest").inc()
    await _notify_sse({"type":"job","id":jid,"status":"queued","created_at":now})
    LOG.info({"event":"enqueue","job_id":jid,"type":"ingest","conn":j.conn_name,"repo_path":j.repo_path,"git_url":j.git_url})
    return {"ok": True, "id": jid}

@app.get("/jobs")
async def list_jobs(status: Optional[str]=None, limit: int=100):
    q = "SELECT * FROM jobs"
    args = []
    if status:
        q += " WHERE status=?"
        args.append(status)
    q += " ORDER BY id DESC LIMIT ?"
    args.append(limit)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(q, args)).fetchall()
    return {"items": [dict(r) for r in rows]}

@app.get("/jobs/{jid}")
async def get_job(jid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        row = await (await db.execute("SELECT * FROM jobs WHERE id=?", (jid,))).fetchone()
        if not row: raise HTTPException(404, "job not found")
    return dict(row)

@app.post("/jobs/{jid}/cancel")
async def cancel_job(jid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE jobs SET status='canceled' WHERE id=? AND status IN ('queued','running')", (jid,))
        await db.commit()
    await _notify_sse({"type":"job","id":jid,"status":"canceled"})
    LOG.info({"event":"cancel","job_id":jid})
    return {"ok": True}

# SSE stream
subscribers = set()
async def _notify_sse(message: dict):
    dead = set()
    for queue in list(subscribers):
        try:
            await queue.put(message)
        except Exception:
            dead.add(queue)
    for d in dead:
        subscribers.discard(d)

@app.get("/events/stream")
async def sse_stream():
    queue = asyncio.Queue()
    subscribers.add(queue)
    async def gen():
        try:
            while True:
                msg = await queue.get()
                yield f"data: {json.dumps(msg)}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            subscribers.discard(queue)
    return StreamingResponse(gen(), media_type="text/event-stream")

# GitHub webhook
def _verify_signature(body: bytes, sig: str) -> bool:
    if not WEBHOOK_SECRET: return True
    sha_name, signature = sig.split("=", 1) if "=" in sig else ("", "")
    if sha_name != "sha256": return False
    mac = hmac.new(WEBHOOK_SECRET.encode(), msg=body, digestmod=hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), signature)

@app.post("/hook/github")
async def github_hook(request: Request):
    body = await request.body()
    sig = request.headers.get("X-Hub-Signature-256", "")
    if not _verify_signature(body, sig):
        raise HTTPException(401, "invalid signature")
    event = request.headers.get("X-GitHub-Event", "unknown")
    payload = await request.json()
    repo = payload.get("repository", {}).get("clone_url")
    branch = (payload.get("ref","").split("/")[-1] if payload.get("ref") else None)
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("INSERT INTO events(created_at, source, key, payload) VALUES (?,?,?,?)",
                         (now, "github", event, json.dumps(payload)))
        await db.execute(
            "INSERT INTO jobs(created_at, scheduled_at, status, type, priority, git_url, git_branch, conn_name, owner) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (now, now, "queued", "ingest", 0, repo, branch, "demo_pg", "")
        )
        await db.commit()
    LOG.info({"event":"webhook","source":"github","repo":repo,"branch":branch})
    await _notify_sse({"type":"event","key":event,"payload":{"repo":repo,"branch":branch},"created_at":now})
    return {"ok": True}

@app.get("/admin/backup")
async def backup_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dst = os.path.join(BACKUP_DIR, f"queue-{ts}.sqlite")
    src_conn = sqlite3.connect(DB_PATH)
    dst_conn = sqlite3.connect(dst)
    with dst_conn:
        src_conn.backup(dst_conn)
    src_conn.close(); dst_conn.close()
    LOG.info({"event":"backup","path":dst})
    return {"ok": True, "path": dst}

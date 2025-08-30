import os, time, datetime, shutil, aiosqlite, asyncio, socket, logging, random
from pathlib import Path
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from queue.logging_util import setup_json_logging

# Import scanner modules (installed in image)
from lineage_scanner.scan import scan_path
from lineage_scanner.config import load_connections
from lineage_scanner.ingest_neo4j import ingest
from neo4j import GraphDatabase

DB_PATH = os.getenv("DB_PATH", "/data/queue.db")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "3"))
CHECKOUT_DIR = os.getenv("CHECKOUT_DIR", "/tmp/checkout")
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "2"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9100"))

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS") or os.getenv("NEO4J_PASSWORD", "")

HOSTNAME = socket.gethostname()
LOG = setup_json_logging("queue.worker")

# Metrics
JOBS_STARTED = Counter("worker_jobs_started_total", "Jobs started")
JOBS_DONE = Counter("worker_jobs_done_total", "Jobs completed successfully")
JOBS_FAILED = Counter("worker_jobs_failed_total", "Jobs failed")
JOBS_REQUEUED = Counter("worker_jobs_requeued_total", "Jobs requeued for retry")
JOB_DURATION = Histogram("worker_job_duration_seconds", "Job runtime seconds")
QUEUE_DEPTH = Gauge("worker_queue_depth", "Queued jobs ready to run")

def now_iso():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def flow_job_id():
    return f"scan-{now_iso()}"

def clean_dir(p):
    try: shutil.rmtree(p, ignore_errors=True)
    except Exception: pass

def backoff_seconds(attempts: int, base: float=15.0, cap: float=3600.0):
    # exponential backoff with jitter, capped
    exp = base * (2 ** max(0, attempts-1))
    jitter = exp * (0.2 * (random.random()-0.5) * 2)  # +/-20%
    return min(cap, max(base, exp + jitter))

async def fetch_and_lock_job(db):
    # Atomically claim one job using an IMMEDIATE transaction
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("BEGIN IMMEDIATE;")
    try:
        now = now_iso()
        cur = await db.execute("SELECT id FROM jobs WHERE status='queued' AND scheduled_at <= ? ORDER BY priority DESC, id ASC LIMIT 1", (now,))
        row = await cur.fetchone()
        if not row:
            await db.execute("COMMIT;")
            return None
        jid = row[0]
        await db.execute("UPDATE jobs SET status='running', started_at=? WHERE id=? AND status='queued'", (now, jid))
        await db.execute("COMMIT;")
        db.row_factory = aiosqlite.Row
        cur2 = await db.execute("SELECT * FROM jobs WHERE id=?", (jid,))
        return await cur2.fetchone()
    except Exception as e:
        await db.execute("ROLLBACK;")
        raise

async def queue_depth(db):
    cur = await db.execute("SELECT COUNT(1) FROM jobs WHERE status='queued' AND scheduled_at <= ?", (now_iso(),))
    n = (await cur.fetchone())[0]
    QUEUE_DEPTH.set(n)

async def run_job(row):
    jid = row["id"]
    t0 = time.time()
    JOBS_STARTED.inc()
    repo_path = row["repo_path"]
    git_url = row["git_url"]
    branch = row["git_branch"]
    conn_name = row["conn_name"] or "demo_pg"
    owner = row["owner"] or ""
    start = now_iso()

    # Prepare code dir
    code_dir = None
    if git_url:
        from git import Repo
        code_dir = os.path.join(CHECKOUT_DIR, f"job-{jid}")
        clean_dir(code_dir)
        LOG.info({"event":"git_clone","job_id":jid,"url":git_url,"branch":branch})
        Repo.clone_from(git_url, code_dir, branch=branch if branch else None)
    else:
        code_dir = repo_path

    LOG.info({"event":"scan_start","job_id":jid,"path":code_dir,"conn":conn_name})
    results = scan_path(code_dir, conn_name, system="postgres", owner=owner)

    LOG.info({"event":"ingest_start","job_id":jid,"neo4j_uri":NEO4J_URI})
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with drv as driver:
        ingest(driver, results['feeds'], results['pdes'], results['flows'], flow_job_id(), start)

    if git_url:
        clean_dir(code_dir)

    dur = time.time() - t0
    JOBS_DONE.inc()
    JOB_DURATION.observe(dur)
    LOG.info({"event":"job_done","job_id":jid,"duration_sec":round(dur,3)})

async def worker_loop(idx: int):
    LOG.info({"event":"worker_start","index":idx,"host":HOSTNAME,"db":DB_PATH})
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            try:
                await queue_depth(db)
                row = await fetch_and_lock_job(db)
                if not row:
                    await asyncio.sleep(POLL_INTERVAL); continue
                jid = row["id"]
                try:
                    await run_job(row)
                    end = now_iso()
                    await db.execute("UPDATE jobs SET status='done', finished_at=?, error=NULL WHERE id=?", (end, jid))
                    await db.commit()
                except Exception as e:
                    LOG.error({"event":"job_error","job_id":jid,"error":str(e)})
                    # retry logic
                    attempts = (row["attempts"] or 0) + 1
                    max_attempts = row["max_attempts"] or 3
                    if attempts < max_attempts:
                        delay = backoff_seconds(attempts)
                        next_time = (datetime.datetime.utcnow() + datetime.timedelta(seconds=delay)).strftime("%Y-%m-%dT%H:%M:%SZ")
                        await db.execute("UPDATE jobs SET status='queued', attempts=?, scheduled_at=?, error=? WHERE id=?",
                                         (attempts, next_time, str(e), jid))
                        await db.commit()
                        JOBS_REQUEUED.inc()
                        LOG.info({"event":"job_requeue","job_id":jid,"attempts":attempts,"next_run":next_time})
                    else:
                        end = now_iso()
                        await db.execute("UPDATE jobs SET status='error', finished_at=?, attempts=?, error=? WHERE id=?",
                                         (end, attempts, str(e), jid))
                        await db.commit()
                        JOBS_FAILED.inc()
                # loop
            except Exception as e:
                LOG.error({"event":"worker_loop_error","error":str(e)})
                await asyncio.sleep(POLL_INTERVAL)

async def main():
    start_http_server(METRICS_PORT)
    tasks = [asyncio.create_task(worker_loop(i)) for i in range(WORKER_CONCURRENCY)]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())

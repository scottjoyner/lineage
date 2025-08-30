import os, time, datetime, shutil, aiosqlite, asyncio, socket
from pathlib import Path
from neo4j import GraphDatabase

# Import scanner modules (installed in image)
from lineage_scanner.scan import scan_path
from lineage_scanner.config import load_connections
from lineage_scanner.ingest_neo4j import ingest

DB_PATH = os.getenv("DB_PATH", "/data/queue.db")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "3"))
CHECKOUT_DIR = os.getenv("CHECKOUT_DIR", "/tmp/checkout")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS") or os.getenv("NEO4J_PASSWORD", "")

HOSTNAME = socket.gethostname()

async def fetch_and_lock_job(db):
    # Find a queued job due now and lock it by transitioning to running
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    async with db.execute("SELECT id FROM jobs WHERE status='queued' AND scheduled_at <= ? ORDER BY priority DESC, id ASC LIMIT 1", (now,)) as cur:
        row = await cur.fetchone()
    if not row:
        return None
    jid = row[0]
    await db.execute("UPDATE jobs SET status='running', started_at=? WHERE id=? AND status='queued'", (now, jid))
    await db.commit()
    # Re-read
    db.row_factory = aiosqlite.Row
    async with db.execute("SELECT * FROM jobs WHERE id=?", (jid,)) as cur2:
        return await cur2.fetchone()

def flow_job_id():
    return f"scan-{datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}"

def clean_dir(p):
    try:
        shutil.rmtree(p, ignore_errors=True)
    except Exception:
        pass

async def run_job(row):
    jid = row["id"]
    git_url = row["git_url"]
    repo_path = row["repo_path"]
    branch = row["git_branch"]
    conn_name = row["conn_name"] or "demo_pg"
    owner = row["owner"] or ""
    start = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Prepare code dir
    code_dir = None
    if git_url:
        from git import Repo
        code_dir = os.path.join(CHECKOUT_DIR, f"job-{jid}")
        clean_dir(code_dir)
        Repo.clone_from(git_url, code_dir, branch=branch if branch else None)
    else:
        code_dir = repo_path

    print(f"[worker] job {jid} scanning {code_dir} (conn={conn_name})")
    results = scan_path(code_dir, conn_name, system="postgres", owner=owner)

    print(f"[worker] job {jid} ingesting into Neo4j {NEO4J_URI}")
    drv = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with drv as driver:
        ingest(driver, results['feeds'], results['pdes'], results['flows'], flow_job_id(), start)

    # cleanup checkout
    if git_url:
        clean_dir(code_dir)

async def main():
    # ensure db exists
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        while True:
            row = await fetch_and_lock_job(db)
            if not row:
                await asyncio.sleep(POLL_INTERVAL); continue
            jid = row["id"]
            try:
                await run_job(row)
                end = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                await db.execute("UPDATE jobs SET status='done', finished_at=?, error=NULL WHERE id=?", (end, jid))
                await db.commit()
                print(f"[worker] job {jid} done")
            except Exception as e:
                end = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                await db.execute("UPDATE jobs SET status='error', finished_at=?, error=? WHERE id=?", (end, str(e), jid))
                await db.commit()
                print(f"[worker] job {jid} error: {e}")

if __name__ == "__main__":
    asyncio.run(main())

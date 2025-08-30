import os, sys, click
from datetime import datetime, timezone
from lineage_scanner.scan import scan_path
from lineage_scanner.emitters import csv_emitter
from lineage_scanner.config import load_connections
from lineage_scanner.models import now_iso

@click.group()
def cli():
    pass

@cli.command("scan")
@click.option("--path", required=True, help="Repo or directory to scan")
@click.option("--conn", required=True, help="Connection name from connections.yaml")
@click.option("--connections", default="connections.yaml", show_default=True, help="Path to connections.yaml")
@click.option("--owner", default="", help="Owner/team label")
@click.option("--soft-key", default="scanner|RepoScan|1.0.0", show_default=True, help="Synthetic software key")
@click.option("--job-id", default=None, help="FlowRun job_id (default: scan-<ISO>)")
@click.option("--out", required=True, help="Output directory for CSVs")
def cmd_scan(path, conn, connections, owner, soft_key, job_id, out):
    conns = load_connections(connections)
    if conn not in conns:
        click.echo(f"Connection '{conn}' not found in {connections}", err=True); sys.exit(2)
    c = conns[conn]
    job_id = job_id or f"scan-{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    ts = now_iso()

    click.echo(f"[scanner] scanning {path} on conn={conn} system={c.system} -> {out}")
    results = scan_path(path, conn, c.system, owner)
    csv_emitter.emit(out, results['feeds'], results['pdes'], results['flows'], soft_key, job_id, ts)
    click.echo(f"[scanner] wrote CSVs in {out} and FlowRun job_id={job_id}")

if __name__ == "__main__":
    cli()

@cli.command("ingest")
@click.option("--path", required=True, help="Repo or directory to scan")
@click.option("--conn", required=True, help="Connection name from connections.yaml")
@click.option("--connections", default="connections.yaml", show_default=True, help="Path to connections.yaml")
@click.option("--owner", default="", help="Owner/team label")
@click.option("--job-id", default=None, help="FlowRun job_id (default: scan-<ISO>)")
def cmd_ingest(path, conn, connections, owner, job_id):
    """Scan a path and ingest directly into Neo4j using env NEO4J_URI/USER/PASS."""
    from datetime import datetime, timezone
    from neo4j import GraphDatabase
    from lineage_scanner.config import load_connections
    from lineage_scanner.scan import scan_path
    from lineage_scanner.models import now_iso
    from lineage_scanner.ingest_neo4j import ingest
    import os, sys

    conns = load_connections(connections)
    if conn not in conns:
        click.echo(f"Connection '{conn}' not found in {connections}", err=True); sys.exit(2)
    c = conns[conn]

    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pw = os.getenv("NEO4J_PASS") or os.getenv("NEO4J_PASSWORD", "")
    if not pw:
        click.echo("Set NEO4J_PASS or NEO4J_PASSWORD for ingestion.", err=True); sys.exit(2)

    job_id = job_id or f"scan-{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
    ts = now_iso()

    click.echo(f"[ingest] URI={uri} user={user} conn={conn} path={path}")
    results = scan_path(path, conn, c.system, owner)

    drv = GraphDatabase.driver(uri, auth=(user, pw))
    with drv as driver:
        ingest(driver, results['feeds'], results['pdes'], results['flows'], job_id, ts)
    click.echo(f"[ingest] done job_id={job_id}")

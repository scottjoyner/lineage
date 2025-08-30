import os, tempfile, asyncio, json, sqlite3
from fastapi.testclient import TestClient
from queue.api.app import app, init_db, DB_PATH

def test_enqueue_and_list(tmp_path, monkeypatch):
    db = tmp_path / "queue.db"
    monkeypatch.setenv("DB_PATH", str(db))
    # Important: Re-import app to pick up new DB_PATH env (in real test you'd refactor settings)
    from importlib import reload
    from queue.api import app as appmod
    reload(appmod)
    client = TestClient(appmod.app)
    # Health
    r = client.get("/healthz"); assert r.status_code == 200
    # Enqueue
    r = client.post("/jobs/ingest", json={"repo_path":"./sample","conn_name":"demo_pg"})
    assert r.status_code == 200
    # List
    r = client.get("/jobs")
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) >= 1

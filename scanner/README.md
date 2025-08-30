# Lineage Scanner (static analysis)

Pluggable scanner that walks a repo or folder and emits lineage CSVs compatible with the Neo4j import in this project.
Understands:
- Raw SQL (`*.sql`) via `sqlglot` (CREATE TABLE AS SELECT, INSERT ... SELECT, simple SELECT)
- dbt projects (`models/*.sql`) with Jinja rendering
- Airflow DAGs (`*.py`) extracting SQL from common operators

## Local usage
```bash
cd scanner
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# define connections
cp connections.yaml connections.local.yaml  # edit if needed

# scan a repo directory (e.g., ../sample-sql)
python cli.py scan --path ../sample-sql --conn demo_pg --connections connections.local.yaml --out ../neo4j/import
```

## Docker
```bash
docker build -t lineage-scanner:local ./scanner
docker run --rm -v $PWD/scanner/connections.yaml:/connections.yaml:ro -v $PWD/sample-repo:/scan:ro -v $PWD/neo4j/import:/out   lineage-scanner:local scan --path /scan --conn demo_pg --connections /connections.yaml --out /out
```

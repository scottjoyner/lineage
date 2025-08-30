from typing import List, Dict, Tuple
from sqlglot import parse_one, exp
from ..models import PDE, Feed

def normalize_table(ident: str, conn_name: str, system: str) -> Tuple[str, str]:
    name = ident.lower().replace('`','').replace('[','').replace(']','')
    parts = [p for p in name.split('.') if p]
    if len(parts) >= 2:
        schema_table = '.'.join(parts[-2:])
    else:
        schema_table = parts[0]
    feed_key = f"{conn_name}|{schema_table}|table"
    return feed_key, schema_table

def column_pde_key(table: str, col: str) -> str:
    return f"{table}.{col}".lower()

def parse_sql(sql_text: str, conn_name: str, system: str, owner: str="") -> Dict:
    results = {"feeds":{}, "pdes":{}, "flows":[]}
    statements = [s for s in sql_text.split(';') if s.strip()]
    for stmt in statements:
        try:
            tree = parse_one(stmt, read=None)
        except Exception:
            continue

        src_tables = set()
        tgt_table = None
        projected = []
        for t in tree.find_all(exp.Table):
            src_tables.add(t.sql(dialect=None, identify=False))

        if isinstance(tree, exp.Create) and tree.this:
            tgt_table = tree.this.sql(dialect=None, identify=False)
        elif isinstance(tree, exp.Insert) and tree.this:
            tgt_table = tree.this.sql(dialect=None, identify=False)

        select = tree.find(exp.Select)
        if select and select.expressions:
            for e in select.expressions:
                alias = e.alias_or_name
                cols = [c.sql(dialect=None, identify=False) for c in e.find_all(exp.Column)]
                projected.append((alias or "", cols))

        for t in list(src_tables) + ([tgt_table] if tgt_table else []):
            if not t: continue
            fk, name = normalize_table(t, conn_name, system)
            results["feeds"].setdefault(fk, Feed(key=fk, name=name, feed_type="table", format=system, system=system, owner=owner))

        if tgt_table:
            tgt_fk, tgt_name = normalize_table(tgt_table, conn_name, system)
            for alias, cols in projected or [("", [])]:
                if not cols:
                    continue
                for c in cols:
                    parts = c.split('.')
                    col = parts[-1]
                    src_tbl = '.'.join(parts[-3:-1]) if len(parts) >= 3 else None
                    if not src_tbl and src_tables:
                        st = list(src_tables)[0]
                        _, src_tbl = normalize_table(st, conn_name, system)
                    if not src_tbl: continue
                    src_pde = column_pde_key(src_tbl, col)
                    tgt_col = alias or col
                    tgt_pde = column_pde_key(tgt_name, tgt_col)
                    results["pdes"].setdefault(src_pde, PDE(key=src_pde, name=src_pde))
                    results["pdes"].setdefault(tgt_pde, PDE(key=tgt_pde, name=tgt_pde))
                    results["flows"].append((src_pde, tgt_pde))
    return results

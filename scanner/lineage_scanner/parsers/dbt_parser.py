import os
from jinja2 import Environment
from ..utils import dbt_refs, dbt_sources, read_text
from .sql_parser import parse_sql

def scan_dbt_project(root: str, conn_name: str, system: str, owner: str=""):
    results = {"feeds":{}, "pdes":{}, "flows":[]}
    models_dir = os.path.join(root, "models")
    if not os.path.isdir(models_dir):
        return results

    env = Environment()
    for dirpath, _, files in os.walk(models_dir):
        for fn in files:
            if not fn.endswith(".sql"): continue
            path = os.path.join(dirpath, fn)
            raw = read_text(path)
            rendered = env.from_string(raw).render()
            parsed = parse_sql(rendered, conn_name, system, owner)
            results["feeds"].update(parsed["feeds"])
            results["pdes"].update(parsed["pdes"])
            results["flows"].extend(parsed["flows"])
    return results

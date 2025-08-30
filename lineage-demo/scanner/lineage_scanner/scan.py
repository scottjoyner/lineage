import os
from .parsers.sql_parser import parse_sql
from .parsers.dbt_parser import scan_dbt_project
from .parsers.airflow_parser import scan_airflow

def scan_path(root: str, conn_name: str, system: str, owner: str=""):
    results = {"feeds":{}, "pdes":{}, "flows":[]}
    for dirpath, _, files in os.walk(root):
        for fn in files:
            if fn.endswith(".sql"):
                path = os.path.join(dirpath, fn)
                sql = open(path, "r", encoding="utf-8", errors="ignore").read()
                parsed = parse_sql(sql, conn_name, system, owner)
                results["feeds"].update(parsed["feeds"])
                results["pdes"].update(parsed["pdes"])
                results["flows"].extend(parsed["flows"])

    dbt = scan_dbt_project(root, conn_name, system, owner)
    results["feeds"].update(dbt["feeds"])
    results["pdes"].update(dbt["pdes"])
    results["flows"].extend(dbt["flows"])

    for dialect, sql, task in scan_airflow(root):
        parsed = parse_sql(sql, conn_name, dialect, owner)
        results["feeds"].update(parsed["feeds"])
        results["pdes"].update(parsed["pdes"])
        results["flows"].extend(parsed["flows"])

    return results

import ast, os
from ..utils import read_text

SQL_OPS = {
    'PostgresOperator': 'postgres',
    'BigQueryInsertJobOperator': 'bigquery',
    'SnowflakeOperator': 'snowflake',
    'MySqlOperator': 'mysql'
}

def extract_strings(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        s = ''
        for v in node.values:
            if isinstance(v, ast.Constant) and isinstance(v.value, str):
                s += v.value
        return [s] if s else []
    return []

def scan_airflow(root: str):
    findings = []
    for dirpath, _, files in os.walk(root):
        for fn in files:
            if not fn.endswith(".py"): continue
            path = os.path.join(dirpath, fn)
            src = read_text(path)
            try:
                tree = ast.parse(src)
            except Exception:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    op = node.func.id
                    if op in SQL_OPS:
                        dialect = SQL_OPS[op]
                        sql_texts = []
                        task_id = None
                        for kw in node.keywords:
                            if kw.arg == "sql":
                                for s in extract_strings(kw.value): sql_texts.append(s)
                            if kw.arg == "task_id":
                                v = extract_strings(kw.value); task_id = v[0] if v else None
                        for s in sql_texts:
                            findings.append((dialect, s, task_id))
    return findings

import os, re, glob
from typing import Iterable

SQL_EXT = (".sql",)
PY_EXT = (".py",)

def iter_files(root: str, patterns: Iterable[str]=("**/*.sql","**/*.py")):
    for pat in patterns:
        for p in glob.glob(os.path.join(root, pat), recursive=True):
            if os.path.isfile(p):
                yield p

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()

REF_RE = re.compile(r"ref\(['\"](?P<name>[^'\"]+)['\"]\)")
SOURCE_RE = re.compile(r"source\(['\"](?P<src>[^'\"]+)['\"]\s*,\s*['\"](?P<table>[^'\"]+)['\"]\)")

def dbt_refs(sql: str):
    return REF_RE.findall(sql)

def dbt_sources(sql: str):
    return SOURCE_RE.findall(sql)

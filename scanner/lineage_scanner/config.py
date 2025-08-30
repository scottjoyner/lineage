import yaml
from .models import Connection

def load_connections(path: str) -> dict[str, Connection]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    out = {}
    for name, v in data.get("connections", {}).items():
        out[name] = Connection(
            name=name,
            system=v.get("system",""),
            host=v.get("host",""),
            db=v.get("db",""),
            schema=v.get("schema",""),
            extra=v.get("extra", {})
        )
    return out

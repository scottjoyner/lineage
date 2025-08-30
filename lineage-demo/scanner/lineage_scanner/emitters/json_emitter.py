import json, os

def emit_jsonl(path: str, payloads):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for obj in payloads:
            f.write(json.dumps(obj) + "\n")

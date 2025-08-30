#!/usr/bin/env bash
set -euo pipefail
python /app/loader.py --nodes ${NORMALIZED_ROOT}/nodes.jsonl --edges ${NORMALIZED_ROOT}/edges.jsonl

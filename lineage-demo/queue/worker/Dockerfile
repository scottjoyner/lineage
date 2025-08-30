FROM python:3.11-slim

RUN useradd -m appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    DB_PATH=/data/queue.db \
    POLL_INTERVAL=3 \
    CHECKOUT_DIR=/tmp/checkout \
    WORKER_CONCURRENCY=2 \
    METRICS_PORT=9100 \
    LOG_LEVEL=INFO

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*

# worker deps
COPY queue/worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# scanner deps (sqlglot, jinja2, pyyaml, etc.)
COPY scanner/requirements.txt /tmp/scanner-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scanner-requirements.txt

# vendor the scanner package into the image (so imports work without pip-installing it)
COPY scanner/lineage_scanner /app/lineage_scanner

# app code
COPY queue/worker/worker.py ./worker.py
COPY queue/logging_util.py ./logging_util.py

RUN mkdir -p /data /tmp/checkout && chown -R appuser:appuser /app /data /tmp/checkout
USER appuser

VOLUME ["/data"]

CMD ["python", "worker.py"]

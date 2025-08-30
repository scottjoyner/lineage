FROM python:3.11-slim

RUN useradd -m appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1     DB_PATH=/data/queue.db     POLL_INTERVAL=3     CHECKOUT_DIR=/tmp/checkout     WORKER_CONCURRENCY=2     METRICS_PORT=9100     LOG_LEVEL=INFO

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ../scanner/ /scanner/
RUN pip install --no-cache-dir /scanner

COPY worker.py ./worker.py
COPY ../logging_util.py /queue/logging_util.py

RUN mkdir -p /data /tmp/checkout && chown -R appuser:appuser /app /data /tmp/checkout
USER appuser

VOLUME ["/data"]

CMD ["python", "worker.py"]

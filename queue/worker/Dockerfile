FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1     DB_PATH=/data/queue.db     POLL_INTERVAL=3     CHECKOUT_DIR=/tmp/checkout

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Bring in the scanner package to call it in-process
COPY ../scanner/ /scanner/
RUN pip install --no-cache-dir /scanner

COPY worker.py .

VOLUME ["/data"]

CMD ["python", "worker.py"]

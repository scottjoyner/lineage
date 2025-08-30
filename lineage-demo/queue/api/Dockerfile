FROM python:3.11-slim

# Create non-root user
RUN useradd -m appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    DB_PATH=/data/queue.db \
    BACKUP_DIR=/backups \
    API_PORT=9000 \
    GITHUB_WEBHOOK_SECRET= \
    LOG_LEVEL=INFO

# requirements from repo-root context
COPY queue/api/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# app code (paths are from repo root)
COPY queue/api/app.py ./app.py
COPY queue/logging_util.py ./logging_util.py

RUN mkdir -p /data /backups && chown -R appuser:appuser /app /data /backups
USER appuser

VOLUME ["/data", "/backups"]

# valid exec-form healthcheck
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD ["python","-c","import os, urllib.request; u='http://127.0.0.1:'+os.getenv('API_PORT','9000')+'/healthz'; urllib.request.urlopen(u, timeout=2)"]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

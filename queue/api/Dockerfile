FROM python:3.11-slim

# Create non-root user
RUN useradd -m appuser
WORKDIR /app
ENV PYTHONUNBUFFERED=1     DB_PATH=/data/queue.db     BACKUP_DIR=/backups     API_PORT=9000     GITHUB_WEBHOOK_SECRET=     LOG_LEVEL=INFO

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./app.py
COPY ../logging_util.py /queue/logging_util.py

RUN mkdir -p /data /backups && chown -R appuser:appuser /app /data /backups
USER appuser

VOLUME ["/data", "/backups"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD-SHELL python -c "import os,urllib.request,sys;u='http://127.0.0.1:'+os.getenv('API_PORT','9000')+'/healthz';urllib.request.urlopen(u,timeout=2)" || exit 1
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

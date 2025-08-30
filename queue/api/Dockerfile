FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1     DB_PATH=/data/queue.db     BACKUP_DIR=/backups     API_PORT=9000     GITHUB_WEBHOOK_SECRET=

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

VOLUME ["/data", "/backups"]

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

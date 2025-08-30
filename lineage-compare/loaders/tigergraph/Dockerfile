FROM python:3.11-slim
RUN useradd -m app && mkdir -p /app && chown -R app:app /app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY loader.py entrypoint.sh /app/
COPY ../common /app/common
RUN chmod +x /app/entrypoint.sh
USER app
ENTRYPOINT ["/app/entrypoint.sh"]

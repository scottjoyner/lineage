FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY compare_lineage.py /app/
ENTRYPOINT ["python","/app/compare_lineage.py"]

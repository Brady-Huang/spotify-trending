FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    redis==5.0.1 \
    clickhouse-driver==0.2.6

COPY api/main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
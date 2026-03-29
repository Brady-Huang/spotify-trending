FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir kafka-python==2.0.2 faker==20.1.0

COPY producer/producer.py .

CMD ["python", "producer.py"]
# Spotify Trending Songs — Data Pipeline

A real-time data pipeline simulating Spotify's trending songs system, based on the system design from [Build Moat](https://buildmoat.org).

## Architecture (Version A — Streaming)
```
Producer → Kafka → Spark Structured Streaming → ClickHouse → Redis → FastAPI → Dashboard
```

## Components

| Component | Tool | Role |
|---|---|---|
| Producer | Python | Simulates client sending play events |
| Message Queue | Kafka | Event buffer |
| Stream Processor | PySpark Structured Streaming | Dedup + session tracking + PlayFact |
| OLAP | ClickHouse | Aggregated Top K storage |
| Cache | Redis | Query result caching |
| API | FastAPI | Top K query endpoint |
| Dashboard | HTML/JS | Visualization |

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Python 3.10+

### Run
```bash
# Start all services
docker-compose up -d

# Install Python dependencies
pip install -r requirements.txt

# Start producer (simulate play events)
python producer/producer.py

# Start stream processor
python processor/stream_processor.py

# Start API
uvicorn api.main:app --reload
```

### API Endpoints
```
GET /top_tracks?dim=country&num_tracks=100
GET /top_tracks?dim=genre&num_tracks=50
```

## System Design Reference

Based on the streaming architecture:
- Events published directly to Kafka (no Event DB needed)
- Spark Structured Streaming handles dedup + session state
- Effective play = 30 seconds threshold (Spotify's official definition)
- ClickHouse materialized views for different time granularities (1m, 1h, 1d)

## Roadmap

- [x] Version A: Streaming pipeline
- [ ] Version B: Lambda Architecture (+ PostgreSQL + Debezium CDC + MinIO + Iceberg + Spark Batch)
# Spotify Trending Songs — Real-Time Data Pipeline

A production-grade real-time data pipeline simulating Spotify's trending songs system, based on the system design from [Build Moat](https://buildmoat.org).

## System Architecture (Version A — Streaming)
```
Producer → Kafka → Spark Structured Streaming → ClickHouse → Redis Cache → FastAPI → Superset
```

### Architecture Diagram
```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐
│   Producer  │────▶│    Kafka    │────▶│  Spark Structured       │
│  (Python)   │     │  (Message   │     │  Streaming (Processor)  │
└─────────────┘     │   Queue)    │     │  - Deduplication        │
                    └─────────────┘     │  - Session Tracking     │
                                        │  - 30s Play Threshold   │
                                        └────────────┬────────────┘
                                                     │
                                                     ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐
│  Superset   │◀────│    Redis    │◀────│      ClickHouse         │
│ (Dashboard) │     │   (Cache)   │     │  - play_facts           │
└─────────────┘     └─────────────┘     │  - play_counts_1m (MV)  │
                          │             └─────────────────────────┘
                          ▼
                    ┌─────────────┐
                    │   FastAPI   │
                    │  (Top K API)│
                    └─────────────┘
```

## System Design

### Functional Requirements
1. Collect listening metrics from clients
2. Provide Top K songs by different dimensions (country, genre)

### Non-Functional Requirements
1. Generate Top K results ASAP after each hour/day ends
2. Scale to 700M MAUs, 100M tracks
3. High accuracy — avoid over/under-counting

### Key Design Decisions

#### Effective Play Count
Following Spotify's official definition:
> A stream is counted when a listener plays a song for **at least 30 seconds**

Implemented via Spark `applyInPandasWithState`:
- Per-session state tracks `accumulated_ms`
- PlayFact emitted when `accumulated_ms >= 30,000ms`
- Deduplication via `event_id`

#### OLAP Storage
ClickHouse with column-oriented storage for efficient analytical queries:
- `play_facts` — raw PlayFact events
- `play_counts_1m` — Materialized View aggregated by minute

#### Hot Shard Simulation
Producer simulates real-world traffic distribution:
- US: 28%, BR: 10%, UK: 8%, TW: 2%, etc.
- Demonstrates the hot shard problem described in the PDF

#### Caching
Redis cache with 60s TTL for Top K query results, reducing ClickHouse load.

## Components

| Component | Tool | Role |
|---|---|---|
| Producer | Python + kafka-python | Simulates client sending play events |
| Message Queue | Kafka + Zookeeper | Event buffer and delivery |
| Stream Processor | PySpark Structured Streaming | Dedup + session tracking + PlayFact |
| OLAP | ClickHouse | Aggregated Top K storage |
| Cache | Redis | Query result caching |
| API | FastAPI | Top K query endpoint |
| Dashboard | Apache Superset | Visualization |

## API Endpoints

### GET /top_tracks
Returns Top K trending songs by dimension.
```
GET /top_tracks?dim={dim}&num_tracks={num_tracks}&window={window}
```

**Parameters:**
| Parameter | Type | Options | Default |
|---|---|---|---|
| `dim` | string | `country`, `genre` | required |
| `num_tracks` | integer | 1-100 | 10 |
| `window` | string | `1h`, `1d` | `1h` |

**Example:**
```bash
curl "http://localhost:8000/top_tracks?dim=country&num_tracks=10&window=1h"
```

**Response:**
```json
{
  "source": "clickhouse",
  "data": [
    {
      "rank": 1,
      "track_id": "track_30",
      "title": "Programmable client-driven standardization",
      "dimension": "US",
      "total_plays": 12
    }
  ]
}
```

### GET /health
```bash
curl "http://localhost:8000/health"
```

## Quick Start

### Prerequisites
- Docker + Docker Compose
- Git

### Run
```bash
# Clone the repo
git clone https://github.com/your-username/spotify-trending.git
cd spotify-trending

# Start all services
docker-compose up -d

# Check all services are healthy
docker-compose ps
```

### Service URLs

| Service | URL |
|---|---|
| FastAPI | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Spark UI | http://localhost:8080 |
| Superset | http://localhost:8088 |
| ClickHouse | http://localhost:8123 |

### Superset Setup (First Time)
```bash
# Initialize database
docker-compose exec superset superset db upgrade

# Create admin account
docker-compose exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname Admin \
  --email admin@example.com \
  --password admin

# Initialize
docker-compose exec superset superset init
```

Login at http://localhost:8088 with `admin / admin`

### Stop
```bash
docker-compose down
```

### Restart
```bash
docker-compose up -d
```

> **Note:** Checkpoint data is persisted in Docker volumes. On restart, the stream processor will resume from Kafka's latest offset.

## Project Structure
```
spotify-trending/
├── producer/
│   ├── producer.py          # Simulates play events with weighted country distribution
│   └── Dockerfile
├── processor/
│   ├── stream_processor.py  # PySpark Structured Streaming processor
│   └── Dockerfile
├── api/
│   ├── main.py              # FastAPI Top K endpoint
│   └── Dockerfile
├── spark/
│   └── Dockerfile           # Custom Spark image with Python dependencies
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## ClickHouse Schema

### play_facts (Raw Events)
```sql
CREATE TABLE play_facts (
    session_id      String,
    user_id         String,
    track_id        String,
    title           String,
    genre           String,
    country         String,
    is_valid        UInt8,           -- 1=valid (>=30s), 0=invalid
    event_timestamp DateTime64(3, 'Asia/Taipei')
) ENGINE = MergeTree()
ORDER BY (event_timestamp, country, genre)
```

### play_counts_1m (Materialized View)
```sql
CREATE MATERIALIZED VIEW play_counts_1m
ENGINE = SummingMergeTree()
ORDER BY (window_start, track_id, country, genre)
AS SELECT
    toStartOfMinute(event_timestamp) AS window_start,
    track_id, title, genre, country,
    count() AS play_count
FROM play_facts
WHERE is_valid = 1
GROUP BY window_start, track_id, title, genre, country
```

## Capacity Estimation

Based on 700M MAUs with 20% daily active users:
```
700M × 20% × 3600s/hr ÷ 5s/event ÷ 24hr
= ~4.2 Billion events/hour
= ~1.2 Million events/second
```

## Roadmap

- [x] Version A: Streaming pipeline
- [ ] Version B: Lambda Architecture
  - [ ] PostgreSQL (Event DB)
  - [ ] Debezium (CDC)
  - [ ] MinIO (S3-compatible storage)
  - [ ] Apache Iceberg (Table format)
  - [ ] Spark Batch Jobs
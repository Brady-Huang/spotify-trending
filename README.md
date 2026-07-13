# Spotify Trending Songs — Real-Time Data Pipeline

A production-grade real-time data pipeline simulating Spotify's trending songs system, based on the system design from [Build Moat](https://buildmoat.org).

## System Architecture

**Streaming (Real-time):**
```
Producer → Kafka → Spark Structured Streaming → ClickHouse → Redis Cache → FastAPI
```

**Batch (Daily):**
```
Airflow DAG (daily @ 1AM) → ClickHouse (play_facts) → daily_trending
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
                    ┌─────────────┐     ┌─────────────────────────┐
                    │    Redis    │◀────│      ClickHouse         │
                    │   (Cache)   │     │  - play_facts           │
                    └──────┬──────┘     │  - play_counts_1m (MV)  │
                           │            │  - daily_trending        │
                           ▼            └────────────▲────────────┘
                    ┌─────────────┐                  │
                    │   FastAPI   │     ┌─────────────────────────┐
                    │  (Top K API)│     │  Airflow DAG            │
                    └─────────────┘     │  (daily @ 1AM)          │
                                        └─────────────────────────┘
```

## System Design

### Functional Requirements
1. Collect listening metrics from clients
2. Provide Top K songs by different dimensions (country, genre)
3. Generate daily trending report

### Non-Functional Requirements
1. Generate Top K results ASAP after each hour/day ends
2. Scale to 700M MAUs, 100M tracks
3. High accuracy — avoid over/under-counting

### Key Design Decisions

#### Effective Play Count
Following Spotify's official definition, a stream is counted when a listener plays a song for at least 30 seconds.

Implemented via Spark `applyInPandasWithState`:
- Per-session state tracks `accumulated_ms`
- PlayFact emitted once when `accumulated_ms >= 30,000ms`
- `play_fact_emitted` flag prevents duplicate counting within the same session

`applyInPandasWithState` was chosen over `mapGroupsWithState` because it allows yielding multiple rows per group update and integrates naturally with pandas, making the session logic easier to reason about.

#### Writing to ClickHouse via `foreachPartition`
Rather than using `foreach` (which creates one connection per row), the processor uses `foreachPartition` to batch all rows in a partition into a single INSERT. This avoids driver OOM issues and reduces connection overhead significantly.

#### OLAP Storage
ClickHouse with column-oriented storage for efficient analytical queries:
- `play_facts` — raw PlayFact events
- `play_counts_1m` — Materialized View aggregated by minute using `SummingMergeTree`
- `daily_trending` — pre-aggregated daily Top 10 by country and genre, populated by Airflow

ClickHouse was chosen over cloud-native OLAP (e.g. BigQuery) because this project is designed to be fully self-hosted and runnable with a single `docker-compose up`. In a production cloud environment, BigQuery or Redshift would be natural alternatives.

#### Redis Cache (Cache-Aside Pattern)
FastAPI uses a cache-aside pattern with 60s TTL:
1. Check Redis for cached result
2. On cache miss, query ClickHouse and write result to Redis
3. Subsequent requests within TTL are served from Redis

This avoids repeated ClickHouse aggregation queries for the same Top K parameters.

#### Airflow Batch Pipeline
A daily Airflow DAG runs at 1AM to compute the previous day's Top 10 trending songs:
- `check_clickhouse_connection` — verifies ClickHouse is reachable before proceeding
- `create_daily_trending_table` — idempotent table creation
- `compute_daily_trending` — aggregates `play_facts` by country and genre, writes to `daily_trending`

Separating batch from streaming allows historical analysis without impacting the real-time query path.

#### Hot Shard Simulation
Producer simulates real-world traffic distribution:
- US: 28%, BR: 10%, UK: 8%, TW: 2%, etc.
- Demonstrates the hot shard problem where high-volume partitions cause uneven Kafka consumer load

## Components

| Component | Tool | Role |
|---|---|---|
| Producer | Python + kafka-python | Simulates client sending play events |
| Message Queue | Kafka + Zookeeper | Event buffer and delivery |
| Stream Processor | PySpark Structured Streaming | Dedup + session tracking + PlayFact |
| OLAP | ClickHouse | Aggregated Top K storage |
| Cache | Redis | Query result caching (cache-aside) |
| API | FastAPI | Top K query endpoint |
| Batch Scheduler | Airflow | Daily trending report pipeline |

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
  "source": "cache",
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

`source` indicates whether the result was served from `cache` (Redis) or `clickhouse` (cache miss).


## Quick Start

### Prerequisites
- Docker + Docker Compose
- Git

### Run
```bash
git clone https://github.com/Brady-Huang/spotify-trending.git
cd spotify-trending

docker compose up -d
```

> Services may take 1-2 minutes to fully start. Use `docker compose ps` to check status.

Airflow is automatically initialized on first run. Login at http://localhost:8090 with `admin / admin`.

### Verify It's Working

Follow the data flow step by step:

**1. Check producer is sending events**
```bash
docker compose logs -f producer
```
You should see play events being emitted every few seconds.

**2. Check processor is consuming from Kafka**
```bash
docker compose logs -f processor
```
You should see Spark batch processing logs.

**3. Check Spark Structured Streaming**

Open http://localhost:4040 → Structured Streaming tab. You should see input/processing rate and batch progress.

**4. Query the Top K API**
```bash
curl "http://localhost:8000/top_tracks?dim=country&num_tracks=5&window=1h"
```

**5. Trigger the daily batch DAG manually**

Open http://localhost:8090, find `daily_trending`, and trigger it manually — no need to wait until 1AM to see the batch results.

### Service URLs

| Service | URL |
|---|---|
| FastAPI | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |
| Spark Application UI | http://localhost:4040 |
| Airflow UI | http://localhost:8090 |
| ClickHouse | http://localhost:8123 |

### Stop
```bash
docker compose down
```

### Restart
```bash
docker compose up -d
```

> **Note:** Checkpoint data is persisted in Docker volumes. On restart, the stream processor will resume from the last committed Kafka offset.

## Project Structure
```
spotify-trending/
├── producer/
│   ├── producer.py          # Simulates play events with weighted country distribution
│   └── Dockerfile
├── processor/
│   ├── stream_processor.py  # PySpark Structured Streaming with stateful session tracking
│   └── Dockerfile
├── api/
│   ├── main.py              # FastAPI Top K endpoint with Redis cache-aside
│   └── Dockerfile
├── airflow/
│   ├── Dockerfile           # Airflow image with clickhouse-driver
│   └── dags/
│       └── daily_trending.py  # Daily batch DAG for trending report
├── spark/
│   └── Dockerfile           # Custom Spark image with Python dependencies
├── terraform/
│   ├── main.tf              # GCP resources (VPC, VM, GCS, firewall)
│   ├── variables.tf
│   ├── outputs.tf
│   └── startup.sh           # VM startup script
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

### daily_trending (Batch Output)
```sql
CREATE TABLE daily_trending (
    report_date     Date,
    dimension_type  String,
    dimension_value String,
    track_id        String,
    title           String,
    total_plays     UInt64,
    rank            UInt32
) ENGINE = MergeTree()
ORDER BY (report_date, dimension_type, rank)
```

## Deploy to GCP

```bash
cd terraform
terraform init
terraform apply
```

Resources created: VPC, subnet, firewall, static IP, GCS bucket, GCE VM (e2-standard-4).
The VM automatically clones this repo and runs `docker compose up` on startup.

## Capacity Estimation

Based on 700M MAUs with 20% daily active users:

Assumptions:
- 20% DAU rate → 140M daily active users
- DAU distributed evenly across 24 hours → at any given hour, 1/24 of DAU are active
- Each active user generates 1 event every 5 seconds

```
700M × 20% ÷ 24hr × 3600s/hr ÷ 5s/event
= ~4.2 Billion events/hour
= ~1.2 Million events/second
```

This project simulates the architecture at this scale. The local Docker Compose setup is for development and demonstration purposes. In a real deployment, Kafka partitions, Spark workers, and ClickHouse shards would scale horizontally to handle ~1.2M events/sec.
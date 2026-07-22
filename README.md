# Spotify Trending Songs — Real-Time Data Pipeline

A production-grade real-time data pipeline simulating Spotify's trending songs system.

📄 **[Design Deep Dive](docs/DESIGN.md)** — detailed architecture diagram, system design, key design decisions (idempotency, dual-sink streaming, Silver layer reconciliation), and full schemas.

## Architecture

```
┌─────────────┐     ┌─────────────┐
│   Producer  │────▶│    Kafka    │
│  (Python)   │     │  (Message   │
└─────────────┘     │   Queue)    │
                    └──────┬──────┘
                           │  (single source, two independent streaming queries)
              ┌────────────┴────────────────┐
              ▼                             ▼
   ┌─────────────────────┐      ┌──────────────────────────┐
   │ Bronze sink           │      │ Serving sink               │
   │ Spark (stateless)    │      │ Spark applyInPandasWithState │
   │ append, no logic     │      │ - Session Tracking        │
   └──────────┬───────────┘      │ - 30s Play Threshold      │
              │                  └──────────┬────────────────┘
              ▼                             ▼
   ┌─────────────────────┐      ┌──────────────────────────┐
   │ Iceberg on MinIO     │      │  ClickHouse               │
   │ raw_events           │      │  play_facts (7-day TTL)   │
   │ MERGE INTO event_id   │      │  probabilistic dedup      │
   │ (exact dedup,        │      │  play_counts_1m (MV)      │
   │  permanent)           │      └──────────┬────────────────┘
   └──────────┬───────────┘                 │
              │                             ▼
              ▼                   ┌─────────────────┐
   ┌─────────────────┐            │  Redis Cache    │
   │     Trino       │            └────────┬────────┘
   └────────┬────────┘                     │
            │                              ▼
   ┌────────▼────────┐            ┌─────────────────┐
   │  Airflow DAG    │            │    FastAPI      │
   │  (daily @ 1AM)  │            │  (Top K API)    │
   └────────┬────────┘            └────────▲────────┘
            │                              │
            ▼                              │
   ┌─────────────────────┐                 │
   │ Iceberg              │                │
   │ play_facts_historical │                │
   │ (Silver, MERGE INTO   │                │
   │  upsert, re-derived   │                │
   │  is_valid)            │                │
   └──────────┬───────────┘                 │
              ▼                             │
   ┌─────────────────┐                     │
   │   ClickHouse    │─────────────────────┘
   │  daily_trending │
   │  (Gold)         │
   └─────────────────┘
```

## System Design

**Functional:** collect listening events from clients; rank Top K songs by dimension (country, genre); generate a daily trending report.

**Non-Functional:** a play only counts after 30 seconds of listening; Top K results update every minute via Materialized View; writes must stay correct under Spark micro-batch retries (idempotency).

Full requirements, production scaling considerations, and capacity estimation (~1.2M events/sec at 700M MAUs): [docs/DESIGN.md](docs/DESIGN.md#system-design)

## Key Design Highlights

A few of the more interesting decisions — full writeups in [docs/DESIGN.md](docs/DESIGN.md#key-design-decisions):

- **[Dual Sink from a Single Kafka Source](docs/DESIGN.md#dual-sink-from-a-single-kafka-source)** — Bronze (lossless, stateless) and serving (best-effort, stateful) run as two independent streaming queries so a problem in one can't compromise the other.
- **[Idempotent Writes](docs/DESIGN.md#idempotent-writes)** — Iceberg uses exact row-level dedup via `MERGE INTO` on `event_id`; ClickHouse uses block-level probabilistic dedup via `non_replicated_deduplication_window`. Both verified with kill tests.
- **[Silver Layer Batch Recomputation](docs/DESIGN.md#silver-layer-batch-recomputation)** — the real-time `is_valid` isn't authoritative; a daily Airflow job re-derives it from the complete Bronze history via `MERGE INTO` upsert.
- **[Why MV dedup has to happen before the write](docs/DESIGN.md#olap-storage)** — `SummingMergeTree` only adds and can't be corrected retroactively, so `insert_deduplication` has to stop duplicates before they ever reach `play_facts`.

## Components

| Component | Tool | Role |
|---|---|---|
| Producer | Python + kafka-python | Simulates client sending play events |
| Message Queue | Kafka + Zookeeper | Event buffer and delivery |
| Stream Processor | PySpark Structured Streaming | Dedup + session tracking + PlayFact |
| OLAP | ClickHouse | Real-time aggregated Top K storage (7-day TTL) |
| Lakehouse | Iceberg on MinIO | Bronze `raw_events` + Silver `play_facts_historical` (permanent) |
| Query Engine | Trino | SQL queries on Iceberg for batch pipeline |
| Cache | Redis | Query result caching (cache-aside) |
| API | FastAPI | Top K query endpoint |
| Batch Scheduler | Airflow | Daily trending report pipeline |
| Monitoring | Prometheus + Grafana | Metrics collection and visualization |

## Monitoring

The pipeline is monitored with Prometheus and Grafana. Metrics are collected from Kafka, ClickHouse, the host system, and Spark Structured Streaming via a custom `StreamingQueryListener` (see [docs/DESIGN.md](docs/DESIGN.md#spark-consumer-lag-monitoring) for why this is needed).

**Spark Streaming Dashboard** — batch duration, consumer lag, input/processed rows/sec, ClickHouse insert rate, CPU usage

![Spark Streaming Dashboard](docs/images/grafana-spark-dashboard.png)

**Node Exporter Dashboard** — CPU, memory, disk, network

![Node Exporter Dashboard](docs/images/grafana-node-dashboard.png)

**ClickHouse Dashboard** — query rate, merge activity, read/write, compressed buffer

![ClickHouse Dashboard](docs/images/grafana-clickhouse-dashboard.png)

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

Once all services are up, the pipeline runs automatically:

1. **Producer** continuously sends simulated play events to Kafka
2. **Spark** runs two independent streaming queries off the same Kafka topic:
   - Bronze sink: writes every raw event to Iceberg `raw_events`, no logic applied
   - Serving sink: tracks each session's listening time and emits a PlayFact to ClickHouse once a user has listened for 30 seconds
3. **ClickHouse** stores the PlayFacts and aggregates them by minute via a Materialized View; raw data expires after 7 days
4. **Iceberg on MinIO** stores all raw events permanently in `raw_events` (Bronze)
5. **FastAPI** serves real-time Top K queries, backed by Redis cache
6. **Airflow** runs a daily batch job at 1AM — recomputes `is_valid` from `raw_events` into `play_facts_historical` (Silver) via Trino, then writes Top 10 to ClickHouse `daily_trending` (Gold)

### Prerequisites
- Docker + Docker Compose
- Git

### Run
```bash
git clone https://github.com/Brady-Huang/spotify-trending.git
cd spotify-trending

docker compose up -d
```

> Services may take 2-3 minutes to fully start. Use `docker compose ps` to check status.

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
You should see both ClickHouse and Iceberg write logs.

**3. Check Spark Structured Streaming**

Open http://localhost:4040 → Structured Streaming tab. You should see input/processing rate and batch progress.

**4. Query the Top K API**
```bash
curl "http://localhost:8000/top_tracks?dim=country&num_tracks=5&window=1h"
```

**5. Explore ClickHouse directly**

Open http://localhost:8123/play to run queries in the browser:

```sql
-- Confirm data is flowing in
SELECT count() FROM play_facts

-- Real-time play counts per minute (Materialized View)
SELECT window_start, sum(play_count) AS plays
FROM play_counts_1m
GROUP BY window_start
ORDER BY window_start DESC
LIMIT 10

-- Daily trending results (after triggering Airflow DAG)
SELECT * FROM daily_trending
WHERE report_date = today()
ORDER BY rank ASC
```

**6. Query Iceberg via Trino**
```bash
docker compose exec trino trino --execute "SELECT count(*) FROM iceberg.spotify.raw_events"
docker compose exec trino trino --execute "SELECT country, count(*) as plays FROM iceberg.spotify.raw_events GROUP BY country ORDER BY plays DESC LIMIT 5"

# Verify Iceberg idempotency: total rows should equal distinct event_id — no duplicates,
# even across Spark retries/kill tests
docker compose exec trino trino --execute "SELECT count(*) AS total, count(DISTINCT event_id) AS distinct_events FROM iceberg.spotify.raw_events"
```

**7. Trigger the daily batch DAG manually**

Open http://localhost:8090, find `spotify_batch_pipeline`, and trigger it manually. It runs `check_connections → create_daily_trending_table → compute_play_facts_historical → compute_daily_trending`. Once complete, verify results:
```bash
curl "http://localhost:8123/?query=SELECT%20*%20FROM%20daily_trending%20ORDER%20BY%20dimension_type%2C%20rank%20ASC%20LIMIT%2020"
```

**8. View monitoring dashboards**

Open http://localhost:3000 (Grafana) with `admin / admin`. Import dashboards:
- Spark Streaming Dashboard (custom)
- Node Exporter Full: ID `1860`
- ClickHouse: ID `882`

**9. Explore MinIO**

Open http://localhost:9001 with `minioadmin / minioadmin`. Browse `warehouse/spotify/raw_events/` to see Iceberg `data/` and `metadata/` directories.

### Service URLs

| Service | URL |
|---|---|
| FastAPI | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Spark Master UI | http://localhost:8080 |
| Spark Application UI | http://localhost:4040 |
| Airflow UI | http://localhost:8090 |
| ClickHouse | http://localhost:8123 |
| MinIO Console | http://localhost:9001 |
| Trino | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |

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
│   ├── stream_processor.py  # PySpark Structured Streaming main entry point
│   ├── clickhouse_writer.py # ClickHouse init and write logic (foreachPartition)
│   ├── iceberg_writer.py    # Iceberg REST catalog config and write logic
│   ├── metrics.py           # Prometheus StreamingQueryListener
│   └── Dockerfile
├── api/
│   ├── main.py              # FastAPI Top K endpoint with Redis cache-aside
│   └── Dockerfile
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── spotify_batch_pipeline.py  # Recomputes Silver from raw_events, writes daily_trending to ClickHouse
├── spark/
│   └── Dockerfile
├── trino/
│   └── catalog/
│       └── iceberg.properties  # Trino Iceberg connector config
├── monitoring/
│   └── prometheus.yml
├── docs/
│   ├── DESIGN.md
│   └── images/
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── startup.sh
├── docker-compose.yml
└── README.md
```

## Deploy to GCP

```bash
cd terraform
terraform init
terraform apply
```

Resources created: VPC, subnet, firewall, static IP, GCS bucket, GCE VM (e2-standard-4).
The VM automatically clones this repo and runs `docker compose up` on startup.

See [docs/DESIGN.md](docs/DESIGN.md#capacity-estimation) for the capacity estimation this project's architecture is sized against (~1.2M events/sec at 700M MAUs).
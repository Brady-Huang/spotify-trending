# Design Deep Dive
 
This document covers the detailed system design, design decisions, and schemas behind the [Spotify Trending Songs pipeline](../README.md). See the README for the architecture diagram, Quick Start, and how to run it.
 
## System Design
 
### Functional Requirements
1. Collect listening metrics from clients
2. Provide Top K songs by different dimensions (country, genre)
3. Generate daily trending report
### Non-Functional Requirements
1. High accuracy — count a play only after 30 seconds of listening
2. Low latency — Top K results updated every minute via Materialized View
3. Idempotent writes — Spark micro-batch retries must not double-count data
### Production Considerations
1. Scale to 700M MAUs (~1.2M events/sec) — requires horizontal scaling of Kafka partitions, Spark executors, and ClickHouse shards
2. Generate Top K results ASAP after each hour/day ends — would require windowed aggregation triggers rather than fixed 1AM batch
## Key Design Decisions
 
### Effective Play Count
Following Spotify's official definition, a stream is counted when a listener plays a song for at least 30 seconds.
 
Implemented via Spark `applyInPandasWithState`:
- Per-session state tracks `accumulated_ms` using `max(accumulated_ms, position_ms)` — idempotent by construction, so replaying the same heartbeat twice never inflates the count
- PlayFact emitted once when `accumulated_ms >= 30,000ms`
- `play_fact_emitted` flag prevents duplicate emission within the same session
`applyInPandasWithState` was chosen over `mapGroupsWithState` because it allows yielding multiple rows per group update and integrates naturally with pandas, making the session logic easier to reason about.
 
### Dual Sink from a Single Kafka Source
Rather than deriving both the Bronze and serving layers from one stateful stream, the processor runs **two independent Structured Streaming queries** against the same Kafka topic, each with its own checkpoint and failure domain:
 
- **Bronze sink** — stateless, `outputMode("append")`, no `applyInPandasWithState`. Every raw heartbeat event is written to Iceberg `raw_events` unmodified. Because it has no state to manage, it isn't affected by state eviction on the other query.
- **Serving sink** — stateful, `applyInPandasWithState`, tracks per-session `accumulated_ms` and emits a `PlayFact` to ClickHouse once the 30s threshold is crossed. Under memory pressure this query is allowed to evict state early, trading a small amount of undercounting for bounded memory and low latency.
This gives the two sinks different correctness guarantees by design: `raw_events` is a lossless, complete capture of the Kafka topic, while `play_facts` is a best-effort, low-latency approximation. Running them as separate queries means a failure or backpressure issue in the stateful serving query can't propagate into the Bronze layer's completeness.
 
### Idempotent Writes
Spark's `foreachBatch` retries a micro-batch on failure (e.g. driver restart) by resending the whole batch — so a batch can be *delivered* to a sink more than once (at-least-once delivery). To keep the result correct despite that, both sinks are written idempotently: a repeated delivery should not change the outcome, even though the write itself happens again. This was verified by kill-testing the processor mid-batch and checking for duplicates on recovery.
 
- **Iceberg — row-level, exact dedup.** `write_to_iceberg` uses `MERGE INTO ... ON target.event_id = source.event_id WHEN NOT MATCHED THEN INSERT`, keyed on the producer-generated globally unique `event_id`. A resent micro-batch is a no-op for rows that already landed.
- **ClickHouse — block-level, probabilistic dedup.** `write_to_clickhouse` inserts with `insert_deduplication=1` and `deduplicate_blocks_in_dependent_materialized_views=1`, backed by `non_replicated_deduplication_window` on the `play_facts` table. ClickHouse deduplicates by fingerprinting inserted blocks within that window rather than guaranteeing per-row uniqueness — weaker than Iceberg's guarantee, but sufficient as long as the window covers Spark's resend range, which is enough for the serving layer's best-effort contract.
### Hot / Cold Data Tiering
Raw events land in two destinations via the two sinks above:
 
- **ClickHouse** (`play_facts`, 7-day TTL) — source for the `play_counts_1m` Materialized View, enabling real-time Top K queries with low latency
- **Iceberg on MinIO** (`raw_events`, permanent) — full-history storage of every raw event, queried by Trino for the daily batch pipeline
This separation keeps ClickHouse lean (only recent data) while preserving full history in the lakehouse for analytics.
 
### Silver Layer Batch Recomputation
Because the serving sink's `is_valid` decision can be affected by state eviction under memory pressure, it isn't authoritative. The daily Airflow DAG recomputes it from the complete Bronze history:
 
- Queries `iceberg.spotify.raw_events` (Bronze) via Trino, groups by `session_id`, and recomputes `is_valid` as `MAX(position_ms) >= 30000`
- Upserts the result into `iceberg.spotify.play_facts_historical` (Silver) via `MERGE INTO` rather than `DELETE + INSERT` — `MERGE INTO` is a single atomic operation, so a mid-task failure can't leave the table in a "deleted but not reinserted" state, and the job is safe to rerun idempotently (e.g. after late-arriving data)
- `daily_trending` (Gold) is computed from `play_facts_historical`, not from the real-time `play_facts` table, so the daily report reflects fully reconciled numbers rather than the streaming layer's approximation
Validated by comparing the recomputed valid-play ratio (70.3%) against the producer's known completion probability (70%) — a statistical sanity check rather than a row-by-row comparison.
 
### Lakehouse Architecture
Iceberg on MinIO provides an open table format for historical data:
- **Open standard** — Spark writes, Trino reads, no vendor lock-in
- **Partitioned** by `days(event_timestamp)` for efficient time-range queries
- **REST Catalog** (`tabulario/iceberg-rest`) manages table metadata, simpler than Hive Metastore for self-hosted setups
### Writing to ClickHouse via `foreachPartition`
Rather than using `foreach` (which creates one connection per row), the processor uses `foreachPartition` to batch all rows in a partition into a single INSERT. This avoids driver OOM issues and reduces connection overhead significantly.
 
### OLAP Storage
ClickHouse with column-oriented storage for efficient analytical queries:
- `play_facts` — best-effort `is_valid` from the real-time serving sink (7-day TTL, source for the `play_counts_1m` Materialized View). This is *not* the authoritative record — see [Silver Layer Batch Recomputation](#silver-layer-batch-recomputation) for why the serving sink's `is_valid` can be wrong under state eviction, and how the daily batch job reconciles it.
- `play_counts_1m` — Materialized View aggregated by minute using `SummingMergeTree`
- `daily_trending` — pre-aggregated daily Top 10 by country and genre, populated by Airflow via Trino from the reconciled `play_facts_historical`, not from `play_facts`
ClickHouse was chosen over cloud-native OLAP (e.g. BigQuery) because this project is designed to be fully self-hosted and runnable with a single `docker compose up`. In a production cloud environment, BigQuery or Redshift would be natural alternatives.
 
**Why deduplication has to happen before the Materialized View, not after.** `play_counts_1m` is a `SummingMergeTree`, which only ever adds — it has no concept of "this row was a duplicate, subtract it back out." A `SummingMergeTree` MV also fires synchronously off the raw block being inserted into `play_facts`: as soon as a block lands, the MV's partial aggregates are pushed forward immediately, and later background merges only combine those partials — they never re-derive them from `play_facts` itself. So if a duplicate block ever reached `play_facts`, the MV's minute-level counts would already be inflated with no path to correct them retroactively. `ReplacingMergeTree` doesn't help here either — replacement only resolves duplicate rows within `play_facts` at merge time (and even then, non-deterministically until `OPTIMIZE ... FINAL`), it doesn't undo what an MV has already summed. This is why `insert_deduplication` has to prevent the duplicate block from ever reaching `play_facts` in the first place — dedup-before-write is the only correctness point that actually protects the MV.
 
### Redis Cache (Cache-Aside Pattern)
FastAPI uses a cache-aside pattern with 60s TTL:
1. Check Redis for cached result
2. On cache miss, query ClickHouse and write result to Redis
3. Subsequent requests within TTL are served from Redis
This avoids repeated ClickHouse aggregation queries for the same Top K parameters.
 
### Airflow Batch Pipeline
`spotify_batch_pipeline` runs daily on the `0 1 * * *` schedule (`catchup=False`) with four sequential tasks:
 
`check_connections >> create_daily_trending_table >> compute_play_facts_historical >> compute_daily_trending`
 
- **`check_connections`** — runs `SELECT 1` against both ClickHouse and Trino before doing any work, so a connectivity problem fails fast instead of partway through the pipeline.
- **`create_daily_trending_table`** — `CREATE TABLE IF NOT EXISTS` for ClickHouse `daily_trending`, safe to run every DAG run.
- **`compute_play_facts_historical`** (Silver) — via Trino, groups `iceberg.spotify.raw_events` by `session_id` over the target day's time range and recomputes `is_valid` as `MAX(position_ms) >= 30000` (the other columns are carried through with `arbitrary()`, since they're constant per session). The result is upserted into `iceberg.spotify.play_facts_historical` with `MERGE INTO`: `WHEN MATCHED` overwrites the row with the freshly computed values, `WHEN NOT MATCHED` inserts it — safe to rerun for the same day (e.g. after late-arriving events) without a `DELETE` step.
- **`compute_daily_trending`** (Gold) — for each dimension (`country`, `genre`), queries `play_facts_historical` filtered to `is_valid = 1` and the target day's range, groups by `(dimension, track_id, title)`, ranks by `total_plays DESC LIMIT 10`, and inserts the ranked rows into ClickHouse `daily_trending`.
Both the Silver and Gold steps currently compute the target day as `datetime.utcnow().date()` (i.e. "today" at the time the DAG runs) rather than the prior full day — this is intentional for now to make manual test runs easier to verify without waiting a full day; switching to the previous day is a straightforward follow-up once the DAG is running on its real schedule.
 
Using Trino to query Iceberg separates the batch analytics path from the real-time query path, keeping ClickHouse focused on low-latency serving.
 
### Spark Consumer Lag Monitoring
Spark Structured Streaming does not register a consumer group with Kafka — it manages offsets via checkpoint. Traditional Kafka monitoring tools (e.g. Burrow, kafka-exporter) cannot observe consumer lag from the broker side.
 
A custom `StreamingQueryListener` exposes `startOffset` and `endOffset` per batch to Prometheus, allowing consumer lag to be derived as `kafka_current_offset - spark_end_offset`.
 
### Hot Shard Simulation
Producer simulates real-world traffic distribution:
- US: 28%, BR: 10%, UK: 8%, TW: 2%, etc.
- Demonstrates the hot shard problem where high-volume partitions cause uneven Kafka consumer load
- Production mitigation: salting (adding random suffix to partition key) to distribute hot keys across multiple partitions
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
 
## ClickHouse Schema
 
### play_facts (Real-Time Storage, 7-day TTL)
```sql
CREATE TABLE play_facts (
    session_id      String,
    user_id         String,
    track_id        String,
    title           String,
    genre           String,
    country         String,
    is_valid        UInt8,
    event_timestamp DateTime64(3, 'UTC')
) ENGINE = MergeTree()
ORDER BY (event_timestamp, country, genre)
TTL toDate(event_timestamp) + INTERVAL 7 DAY
SETTINGS non_replicated_deduplication_window = 1000
```
Writes use `insert_deduplication=1` and `deduplicate_blocks_in_dependent_materialized_views=1` so retried inserts (and the downstream `play_counts_1m` MV) don't double-count.
 
### play_counts_1m (Materialized View, decoupled from source table)
```sql
CREATE TABLE play_counts_1m (
    window_start  DateTime('UTC'),
    track_id      String,
    title         String,
    genre         String,
    country       String,
    play_count    UInt64
) ENGINE = SummingMergeTree()
ORDER BY (window_start, track_id, title, country, genre)
 
CREATE MATERIALIZED VIEW play_counts_1m_mv
TO play_counts_1m
AS SELECT
    toStartOfMinute(event_timestamp) AS window_start,
    track_id, title, genre, country,
    count() AS play_count
FROM play_facts
WHERE is_valid = 1
GROUP BY window_start, track_id, title, genre, country
```
 
Using `TO play_counts_1m` (an explicit target table) instead of an implicit engine on the view itself — dropping and recreating the view logic later won't destroy accumulated historical data, since the data lives in a table you control, not a hidden one managed by the view.
 
### daily_trending (Gold Layer, Batch Output)
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
 
## Iceberg Schema
 
### raw_events (Bronze, Permanent, Immutable)
Every Kafka message, written as-is with no business logic — the source of truth the Silver layer is re-derived from.
```sql
CREATE TABLE iceberg.spotify.raw_events (
    event_id        STRING,
    session_id      STRING,
    user_id         STRING,
    track_id        STRING,
    title           STRING,
    genre           STRING,
    country         STRING,
    position_ms     BIGINT,
    state           STRING,
    event_timestamp TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_timestamp))
```
Idempotent writes via `MERGE INTO ... ON target.event_id = source.event_id WHEN NOT MATCHED THEN INSERT` — duplicates are discarded, never updated, since raw events are immutable historical facts.
 
### play_facts_historical (Silver, Permanent, Recomputed Nightly)
One row per session, with `is_valid` recomputed daily from the full `raw_events` history.
```sql
CREATE TABLE iceberg.spotify.play_facts_historical (
    session_id      VARCHAR,
    user_id         VARCHAR,
    track_id        VARCHAR,
    title           VARCHAR,
    genre           VARCHAR,
    country         VARCHAR,
    is_valid        INTEGER,
    event_timestamp TIMESTAMP
)
```
Upserted daily via `MERGE INTO ... ON session_id WHEN MATCHED THEN UPDATE` — not append-only, since a session's `is_valid` conclusion can legitimately change as late-arriving raw events are reprocessed.
 
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

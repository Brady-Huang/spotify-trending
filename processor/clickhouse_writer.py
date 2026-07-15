import os
import logging
import clickhouse_driver

logger = logging.getLogger("SpotifyProcessor")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")

def init_clickhouse():
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    
    client.execute("""
        CREATE TABLE IF NOT EXISTS play_facts (
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
    """)

    client.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS play_counts_1m
        ENGINE = SummingMergeTree()
        ORDER BY (window_start, track_id, country, genre)
        AS SELECT
            toStartOfMinute(event_timestamp) AS window_start,
            track_id,
            title,
            genre,
            country,
            count() AS play_count
        FROM play_facts
        WHERE is_valid = 1
        GROUP BY window_start, track_id, title, genre, country
    """)
    logger.info("[ClickHouse] Tables ready ✅")


def write_to_clickhouse(batch_df, batch_id):
    row_count = batch_df.count()

    def send_to_clickhouse(partition_iterator):
        client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
        data = []
        for r in partition_iterator:
            if r.is_valid == 1:
                data.append({
                    "session_id": r.session_id,
                    "user_id": r.user_id,
                    "track_id": r.track_id,
                    "title": r.title,
                    "genre": r.genre,
                    "country": r.country,
                    "is_valid": r.is_valid,
                    "event_timestamp": r.event_timestamp
                })
        if data:
            client.execute(
                "INSERT INTO play_facts (session_id, user_id, track_id, title, genre, country, is_valid, event_timestamp) VALUES",
                data
            )

    # 使用 foreachPartition 防止 Driver 記憶體爆炸
    batch_df.foreachPartition(send_to_clickhouse)
    if row_count > 0:
        logger.info(f"🚀 [Batch {batch_id}] Successfully wrote {row_count} rows to ClickHouse.")
    else:
        logger.info(f"💤 [Batch {batch_id}] No data to process.")
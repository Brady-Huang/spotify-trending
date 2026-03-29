from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import clickhouse_driver
import json

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "play-events"
CLICKHOUSE_HOST = "localhost"
PLAY_THRESHOLD_MS = 30000
CHECKPOINT_DIR = "/tmp/spark-checkpoint"

# --- Schema ---
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("track_id", StringType()),
    StructField("title", StringType()),
    StructField("genre", StringType()),
    StructField("country", StringType()),
    StructField("position_ms", LongType()),
    StructField("state", StringType()),
    StructField("timestamp", DoubleType()),
])

# PlayFact schema（輸出）
PLAY_FACT_SCHEMA = StructType([
    StructField("session_id", StringType()),
    StructField("user_id", StringType()),
    StructField("track_id", StringType()),
    StructField("title", StringType()),
    StructField("genre", StringType()),
    StructField("country", StringType()),
    StructField("is_valid", BooleanType()),
])

def create_spark_session():
    return SparkSession.builder \
        .appName("SpotifyTrendingProcessor") \
        .master("spark://localhost:7077") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def update_session_state(session_id, events, state: GroupState):
    """
    對應 PDF 的 stream processor 邏輯：
    - 維護 per-session accumulated_ms
    - 超過 30 秒門檻時產生 PlayFact
    """
    # 取得目前 state，沒有就初始化
    if state.exists:
        current = json.loads(state.get)
    else:
        current = {
            "accumulated_ms": 0,
            "track_id": None,
            "title": None,
            "genre": None,
            "country": None,
            "user_id": None,
            "play_fact_emitted": False,
            "seen_event_ids": []
        }

    results = []

    for event in events:
        # 1. Deduplication
        if event.event_id in current["seen_event_ids"]:
            continue
        current["seen_event_ids"].append(event.event_id)

        # 2. 更新 session metadata
        current["track_id"] = event.track_id
        current["title"] = event.title
        current["genre"] = event.genre
        current["country"] = event.country
        current["user_id"] = event.user_id

        # 3. 累積播放時間
        if event.state == "play":
            current["accumulated_ms"] += 5000

        # 4. 達到 30 秒門檻，產生 PlayFact
        if (current["accumulated_ms"] >= PLAY_THRESHOLD_MS
                and not current["play_fact_emitted"]):
            results.append((
                session_id,
                current["user_id"],
                current["track_id"],
                current["title"],
                current["genre"],
                current["country"],
                True
            ))
            current["play_fact_emitted"] = True
            print(f"✅ PlayFact: {current['title']} | {current['country']}")

        # 5. Session 結束，清理 state
        if event.state == "stop":
            if not current["play_fact_emitted"]:
                # 無效播放，還是輸出一筆記錄但 is_valid=False
                results.append((
                    session_id,
                    current["user_id"],
                    current["track_id"],
                    current["title"],
                    current["genre"],
                    current["country"],
                    False
                ))
            state.remove()
            return iter(results)

    # Session 還沒結束，更新 state
    state.update(json.dumps(current))
    return iter(results)

def write_to_clickhouse(batch_df, batch_id):
    """foreachBatch：把每個 micro-batch 寫入 ClickHouse"""
    rows = batch_df.filter(col("is_valid") == True).collect()

    if not rows:
        return

    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)

    # 建立表（如果不存在）
    client.execute("""
        CREATE TABLE IF NOT EXISTS play_facts (
            session_id   String,
            user_id      String,
            track_id     String,
            title        String,
            genre        String,
            country      String,
            played_at    DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (played_at, country, genre)
    """)

    client.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS play_counts_1m
        ENGINE = SummingMergeTree()
        ORDER BY (window_start, track_id, country, genre)
        POPULATE
        AS SELECT
            toStartOfMinute(played_at) AS window_start,
            track_id,
            title,
            genre,
            country,
            count() AS play_count
        FROM play_facts
        GROUP BY window_start, track_id, title, genre, country
    """)

    data = [{
        "session_id": r.session_id,
        "user_id": r.user_id,
        "track_id": r.track_id,
        "title": r.title,
        "genre": r.genre,
        "country": r.country,
    } for r in rows]

    client.execute(
        "INSERT INTO play_facts (session_id, user_id, track_id, title, genre, country) VALUES",
        data
    )
    print(f"[ClickHouse] Batch {batch_id}: wrote {len(data)} PlayFacts")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[Processor] Reading from Kafka...")

    # 從 Kafka 讀取事件
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 解析 JSON
    events_df = raw_df.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # Dedup + Session State
    play_facts_df = events_df \
        .groupBy("session_id") \
        .applyInPandasWithState(
            update_session_state,
            PLAY_FACT_SCHEMA,
            "session_id string, accumulated_ms long, play_fact_emitted boolean",
            "Update",
            GroupStateTimeout.NoTimeout
        )

    # 寫入 ClickHouse
    query = play_facts_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("[Processor] Streaming started! Waiting for events...")
    query.awaitTermination()

if __name__ == "__main__":
    main()

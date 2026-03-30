import os
import time
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import clickhouse_driver

# --- 配置區 ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "play-events"
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
PLAY_THRESHOLD_MS = 30000

# 設定日誌格式：時間 - 層級 - 訊息
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SpotifyProcessor")

# 1. 定義 Kafka 輸入 Schema
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

# 2. 定義 輸出到 ClickHouse 的 Schema
PLAY_FACT_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("country", StringType(), True),
    StructField("is_valid", IntegerType(), True),     
    StructField("event_timestamp", TimestampType(), True)
])

def create_spark_session():
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    return SparkSession.builder \
        .appName("SpotifyTrendingProcessor") \
        .master(spark_master) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.backpressure.initialRate", "1000") \
        .getOrCreate()

def update_session_state(session_id, pdf_iterator, state: GroupState):
    s_id = str(session_id[0]) if isinstance(session_id, tuple) else str(session_id)

    if state.exists:
        accumulated_ms, play_fact_emitted = state.get
    else:
        accumulated_ms = 0
        play_fact_emitted = False

    results = []
    cols = PLAY_FACT_SCHEMA.names
    for df in pdf_iterator:
        for row in df.itertuples():
            raw_ts = getattr(row, 'timestamp', None)
            if raw_ts:
                # 先轉為 UTC，再轉為台北時區，最後移除時區標籤以符合 Spark TimestampType
                event_ts = pd.to_datetime(float(raw_ts), unit='s', utc=True) \
                            .tz_convert('Asia/Taipei') \
                            .tz_localize(None)
            else:
                event_ts = pd.Timestamp.now()

            current_state = getattr(row, 'state', None)
            if current_state == "play":
                accumulated_ms += 5000

            if accumulated_ms >= PLAY_THRESHOLD_MS and not play_fact_emitted:
                results.append({
                    "session_id": s_id,
                    "user_id": str(getattr(row, 'user_id', '')),
                    "track_id": str(getattr(row, 'track_id', '')),
                    "title": str(getattr(row, 'title', '')),
                    "genre": str(getattr(row, 'genre', '')),
                    "country": str(getattr(row, 'country', '')),
                    "is_valid": 1,
                    "event_timestamp": event_ts
                })
                play_fact_emitted = True
                logger.info(f"PlayFact Emitted: {getattr(row, 'title', '')} ({getattr(row, 'country', '')})")

            if current_state == "stop":
                if not play_fact_emitted:
                    results.append({
                        "session_id": s_id,
                        "user_id": str(getattr(row, 'user_id', '')),
                        "track_id": str(getattr(row, 'track_id', '')),
                        "title": str(getattr(row, 'title', '')),
                        "genre": str(getattr(row, 'genre', '')),
                        "country": str(getattr(row, 'country', '')),
                        "is_valid": 0,
                        "event_timestamp": event_ts
                    })
                state.remove()
                if results:
                    yield pd.DataFrame(results)[cols]
                return

        state.update((accumulated_ms, play_fact_emitted))
        if results:
            yield pd.DataFrame(results)[cols]

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
            event_timestamp DateTime64(3, 'Asia/Taipei')
        ) ENGINE = MergeTree()
        ORDER BY (event_timestamp, country, genre)
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

def main():
    init_clickhouse()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "2000")\
        .option("failOnDataLoss", "false") \
        .load()

    events_df = raw_df.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # 狀態處理：確保 groupBy 後進行狀態運算
    play_facts_df = events_df \
        .groupBy("session_id") \
        .applyInPandasWithState(
            update_session_state,
            outputStructType=PLAY_FACT_SCHEMA,
            stateStructType="accumulated_ms long, play_fact_emitted boolean",
            outputMode="Update",
            timeoutConf=GroupStateTimeout.NoTimeout
        )

    query = play_facts_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

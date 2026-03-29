import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
import clickhouse_driver

# --- 配置區 ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "play-events"
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
PLAY_THRESHOLD_MS = 30000
CHECKPOINT_DIR = "/tmp/spark-checkpoint-v2"

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
        .getOrCreate()

def update_session_state(session_id, pdf_iterator, state: GroupState):
    """
    1. 解決 PySparkTypeError (使用 yield)
    2. 解決 Missing columns: event_timestamp (從原始數據提取)
    3. 解決 is_valid 欄位對齊 (使用 UInt8 數值)
    """
    s_id = str(session_id[0]) if isinstance(session_id, tuple) else str(session_id)

    # 讀取狀態
    if state.exists:
        accumulated_ms, play_fact_emitted = state.get
    else:
        accumulated_ms = 0
        play_fact_emitted = False

    results = []

    for df in pdf_iterator:
        for row in df.itertuples():
            # --- 時間戳記處理 ---
            # 優先從原始 row 抓取 'timestamp' 欄位
            raw_ts = getattr(row, 'timestamp', None)
            # 如果原始資料有時間就用原始的，沒有才用處理當下的時間 (Fallback)
            event_ts = pd.to_datetime(raw_ts, unit='s') if raw_ts else datetime.now()

            # --- 播放狀態邏輯 ---
            current_state = getattr(row, 'state', None)
            if current_state == "play":
                accumulated_ms += 5000 # 假設每 5 秒一筆

            # --- 達標判定 (30秒) ---
            if accumulated_ms >= 30000 and not play_fact_emitted:
                results.append({
                    "session_id": s_id,
                    "user_id": str(getattr(row, 'user_id', '')),
                    "track_id": str(getattr(row, 'track_id', '')),
                    "title": str(getattr(row, 'title', '')),
                    "genre": str(getattr(row, 'genre', '')),
                    "country": str(getattr(row, 'country', '')),
                    "is_valid": 1,  # ClickHouse 用 1 代表 True
                    "event_timestamp": event_ts
                })
                play_fact_emitted = True

            # --- Stop 訊號處理 ---
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
                yield pd.DataFrame(results) if results else pd.DataFrame(columns=PLAY_FACT_SCHEMA.names)
                return

    # 保存狀態並回傳
    state.update((accumulated_ms, play_fact_emitted))
    
    if not results:
        yield pd.DataFrame(columns=PLAY_FACT_SCHEMA.names)
    else:
        yield pd.DataFrame(results)

def init_clickhouse():
    """在啟動前執行一次，確保 Table 與 Materialized View 結構對齊最新邏輯"""
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    
    # 1. 建立原始事實表 (Raw Facts)
    # 我們將 played_at 改為 event_timestamp，並增加 is_valid
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

    # 2. 建立每分鐘統計視圖 (Materialized View)
    # 注意：我們只統計 is_valid = 1 (聽滿 30 秒) 的數據
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
    print("[ClickHouse] DDL initialized with Event-Time and Validation logic.")


def write_to_clickhouse(batch_df, batch_id):
    # 只過濾出有效播放且轉換為 row list
    rows = batch_df.collect()
    if not rows:
        return

    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    data = [{
        "session_id": r.session_id,
        "user_id": r.user_id,
        "track_id": r.track_id,
        "title": r.title,
        "genre": r.genre,
        "country": r.country,
        "is_valid": r.is_valid,
        "event_timestamp": r.event_timestamp
    } for r in rows]
    client.execute(
        "INSERT INTO play_facts (session_id, user_id, track_id, title, genre, country, is_valid, event_timestamp) VALUES",
        data
    )
    print(f"🚀 [ClickHouse] Batch {batch_id}: Inserted {len(data)} records.")
    
def main():
    # 初始化 ClickHouse Table
    init_clickhouse()
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Processor] Connecting to Kafka: {KAFKA_BROKER}")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    events_df = raw_df.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    # 狀態處理
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

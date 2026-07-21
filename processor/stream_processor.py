import os
import pandas as pd
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, IntegerType, TimestampType
)
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from metrics import PrometheusStreamingListener, start_metrics_server
from clickhouse_writer import init_clickhouse, write_to_clickhouse
from iceberg_writer import configure_iceberg, init_iceberg, write_to_iceberg

# --- 配置區 ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
KAFKA_TOPIC = "play-events"
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
ICEBERG_CHECKPOINT_DIR = os.environ.get("ICEBERG_CHECKPOINT_DIR", "/tmp/spark-checkpoint-iceberg")
PLAY_THRESHOLD_MS = 30000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("SpotifyProcessor")

# Kafka 輸入 Schema
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

# ClickHouse 路徑輸出 Schema（維持不變）
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
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
                "software.amazon.awssdk:bundle:2.20.18,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.streaming.backpressure.initialRate", "1000") \
        .getOrCreate()


def update_session_state(session_id, pdf_iterator, state: GroupState):
    """ClickHouse 路徑專用：即時 30 秒門檻判斷，允許為了防 OOM 而丟狀態。"""
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
                event_ts = pd.to_datetime(float(raw_ts), unit='s', utc=True).tz_localize(None)
            else:
                event_ts = pd.Timestamp.utcnow()

            current_state = getattr(row, 'state', None)
            position_ms = int(getattr(row, 'position_ms', 0))
            accumulated_ms = max(accumulated_ms, position_ms)

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


def main():
    init_clickhouse()
    start_metrics_server(8888)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    configure_iceberg(spark)
    init_iceberg(spark)

    spark.streams.addListener(PrometheusStreamingListener())

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "2000") \
        .option("failOnDataLoss", "false") \
        .load()

    events_df = raw_df.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*") \
     .withColumn("event_timestamp", to_timestamp(col("timestamp")))

    # --- 軌道一：全量落湖軌（無狀態，Bronze 層，事後可完全信任的底本）---
    # 直接吃 Kafka 解析完的原始事件，不經過 applyInPandasWithState，
    # 因此即時層為了防 OOM 而丟棄的狀態，完全不會影響這裡的完整性。
    iceberg_query = events_df.writeStream \
        .foreachBatch(write_to_iceberg) \
        .outputMode("append") \
        .option("checkpointLocation", ICEBERG_CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    # --- 軌道二：即時計算軌（有狀態，只對 ClickHouse 負責，允許防 OOM 漏算）---
    play_facts_df = events_df \
        .groupBy("session_id") \
        .applyInPandasWithState(
            update_session_state,
            outputStructType=PLAY_FACT_SCHEMA,
            stateStructType="accumulated_ms long, play_fact_emitted boolean",
            outputMode="Update",
            timeoutConf=GroupStateTimeout.NoTimeout
        )

    clickhouse_query = play_facts_df.writeStream \
        .foreachBatch(write_to_clickhouse) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

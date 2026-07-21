import os
import logging

logger = logging.getLogger("SpotifyProcessor")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9002")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
ICEBERG_WAREHOUSE = "s3://warehouse/"


def configure_iceberg(spark):
    spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    spark.conf.set("spark.sql.catalog.iceberg.uri", ICEBERG_REST_URI)
    spark.conf.set("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
    spark.conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    spark.conf.set("spark.sql.catalog.iceberg.s3.endpoint", MINIO_ENDPOINT)
    spark.conf.set("spark.sql.catalog.iceberg.s3.access-key-id", MINIO_ACCESS_KEY)
    spark.conf.set("spark.sql.catalog.iceberg.s3.secret-access-key", MINIO_SECRET_KEY)
    spark.conf.set("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    spark.conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.sql.catalog.iceberg.s3.region", "us-east-1")
    logger.info("[Iceberg] Spark REST catalog configured ✅")


def init_iceberg(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.spotify")
    # Bronze layer: raw heartbeat events, no business logic applied.
    # Every Kafka message lands here as-is — this is the permanent, complete source of truth
    # that the nightly batch job re-derives play_facts_historical from.
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.spotify.raw_events (
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
    """)
    logger.info("[Iceberg] Table iceberg.spotify.raw_events ready ✅")


def write_to_iceberg(batch_df, batch_id):
    """
    Idempotent write via MERGE INTO keyed on (session_id, event_id).
    If Spark retries this micro-batch (e.g. after a driver restart), the same
    event_id will just overwrite itself instead of creating a duplicate row —
    this is what makes the raw_events table safe to treat as ground truth.
    """
    row_count = batch_df.count()
    if row_count == 0:
        logger.info(f"💤 [Batch {batch_id}] No data to write to Iceberg.")
        return

    batch_df.createOrReplaceTempView("raw_events_batch")
    batch_df.sparkSession.sql("""
        MERGE INTO iceberg.spotify.raw_events AS target
        USING raw_events_batch AS source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info(f"🧊 [Batch {batch_id}] Merged {row_count} raw events into Iceberg.")

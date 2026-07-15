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
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.spotify.play_facts (
            session_id      STRING,
            user_id         STRING,
            track_id        STRING,
            title           STRING,
            genre           STRING,
            country         STRING,
            is_valid        INT,
            event_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_timestamp), country)
    """)
    logger.info("[Iceberg] Table iceberg.spotify.play_facts ready ✅")


def write_to_iceberg(batch_df, batch_id):
    row_count = batch_df.count()
    if row_count > 0:
        batch_df.writeTo("iceberg.spotify.play_facts").append()
        logger.info(f"🧊 [Batch {batch_id}] Successfully wrote {row_count} rows to Iceberg.")
    else:
        logger.info(f"💤 [Batch {batch_id}] No data to write to Iceberg.")

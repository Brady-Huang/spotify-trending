import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_driver
import trino

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_connections():
    # Confirm ClickHouse is reachable
    ch_client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    ch_client.execute("SELECT 1")

    # Confirm Trino is reachable
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    cursor.fetchall()


def create_daily_trending_table():
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    client.execute("""
        CREATE TABLE IF NOT EXISTS daily_trending (
            report_date     Date,
            dimension_type  String,
            dimension_value String,
            track_id        String,
            title           String,
            total_plays     UInt64,
            rank            UInt32
        ) ENGINE = MergeTree()
        ORDER BY (report_date, dimension_type, rank)
    """)


def compute_play_facts_historical(**context):
    """
    Silver layer: re-derives each session's final is_valid conclusion from
    raw_events (Bronze, raw heartbeat detail), and writes it to
    play_facts_historical.

    Uses MERGE INTO (a true upsert) instead of DELETE+INSERT, because:
    - This task must be safe to rerun (e.g. recomputing the same day after
      late-arriving data)
    - DELETE+INSERT is two non-atomic operations — a failure in between can
      leave the data missing for a moment
    - MERGE INTO is a single atomic operation: WHEN MATCHED overwrites with
      the freshly computed result, WHEN NOT MATCHED inserts it — there's no
      "deleted but not reinserted" intermediate state
    """
    report_date = datetime.utcnow().date()
    start = datetime.combine(report_date, datetime.min.time())
    end = start + timedelta(days=1)

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow")
    cursor = conn.cursor()

    # Create the Silver layer table if it doesn't exist yet
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS iceberg.spotify.play_facts_historical (
            session_id      VARCHAR,
            user_id         VARCHAR,
            track_id        VARCHAR,
            title           VARCHAR,
            genre           VARCHAR,
            country         VARCHAR,
            is_valid        INTEGER,
            event_timestamp TIMESTAMP
        )
    """)
    cursor.fetchall()

    # For every session in today's time range, determine is_valid via
    # MAX(position_ms) >= 30000, then upsert the result into
    # play_facts_historical.
    cursor.execute(f"""
        MERGE INTO iceberg.spotify.play_facts_historical AS target
        USING (
            SELECT
                session_id,
                arbitrary(user_id)   AS user_id,
                arbitrary(track_id)  AS track_id,
                arbitrary(title)     AS title,
                arbitrary(genre)     AS genre,
                arbitrary(country)   AS country,
                CASE WHEN max(position_ms) >= 30000 THEN 1 ELSE 0 END AS is_valid,
                max(event_timestamp) AS event_timestamp
            FROM iceberg.spotify.raw_events
            WHERE event_timestamp >= TIMESTAMP '{start}'
              AND event_timestamp <  TIMESTAMP '{end}'
            GROUP BY session_id
        ) AS source
        ON target.session_id = source.session_id
        WHEN MATCHED THEN UPDATE SET
            user_id = source.user_id,
            track_id = source.track_id,
            title = source.title,
            genre = source.genre,
            country = source.country,
            is_valid = source.is_valid,
            event_timestamp = source.event_timestamp
        WHEN NOT MATCHED THEN INSERT (
            session_id, user_id, track_id, title, genre, country, is_valid, event_timestamp
        ) VALUES (
            source.session_id, source.user_id, source.track_id, source.title,
            source.genre, source.country, source.is_valid, source.event_timestamp
        )
    """)
    cursor.fetchall()


def compute_daily_trending(**context):
    """
    Gold layer: queries play_facts_historical (Silver, already-reconciled
    session conclusions), computes each dimension's daily Top 10, and writes
    it to ClickHouse daily_trending for API serving.
    """
    report_date = datetime.utcnow().date()
    start = datetime.combine(report_date, datetime.min.time())
    end = start + timedelta(days=1)

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow")
    cursor = conn.cursor()

    ch_client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)

    for dim in ["country", "genre"]:
        cursor.execute(f"""
            SELECT
                '{report_date}' AS report_date,
                '{dim}'         AS dimension_type,
                {dim}           AS dimension_value,
                track_id,
                title,
                count(*)        AS total_plays
            FROM iceberg.spotify.play_facts_historical
            WHERE is_valid = 1
              AND event_timestamp >= TIMESTAMP '{start}'
              AND event_timestamp <  TIMESTAMP '{end}'
            GROUP BY {dim}, track_id, title
            ORDER BY total_plays DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()

        data = [
            {
                "report_date": report_date,
                "dimension_type": dim,
                "dimension_value": r[2],
                "track_id": r[3],
                "title": r[4],
                "total_plays": r[5],
                "rank": i + 1,
            }
            for i, r in enumerate(rows)
        ]

        if data:
            ch_client.execute("INSERT INTO daily_trending VALUES", data)


with DAG(
    dag_id="spotify_batch_pipeline",
    default_args=default_args,
    description="Precompute play_facts_historical from raw_events, then compute daily Top 10 trending songs to ClickHouse",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    check_conn = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections,
    )

    create_table = PythonOperator(
        task_id="create_daily_trending_table",
        python_callable=create_daily_trending_table,
    )

    compute_historical = PythonOperator(
        task_id="compute_play_facts_historical",
        python_callable=compute_play_facts_historical,
    )

    compute = PythonOperator(
        task_id="compute_daily_trending",
        python_callable=compute_daily_trending,
    )

    check_conn >> create_table >> compute_historical >> compute
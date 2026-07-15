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
    # 確認 ClickHouse 可連
    ch_client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    ch_client.execute("SELECT 1")

    # 確認 Trino 可連
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
    )
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

def compute_daily_trending(**context):
    execution_date = context["execution_date"]
    report_date = execution_date.date()
    start = datetime.combine(report_date, datetime.min.time())
    end = start + timedelta(days=1)

    # 用 Trino 查 Iceberg
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
    )
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
            FROM iceberg.spotify.play_facts
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
            ch_client.execute(
                "INSERT INTO daily_trending VALUES",
                data
            )

with DAG(
    dag_id="daily_trending_report",
    default_args=default_args,
    description="Compute daily Top 10 trending songs from Iceberg via Trino, write to ClickHouse",
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

    compute = PythonOperator(
        task_id="compute_daily_trending",
        python_callable=compute_daily_trending,
    )

    check_conn >> create_table >> compute
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_driver

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def check_clickhouse_connection():
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)
    client.execute("SELECT 1")

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
    report_date = execution_date.date() - timedelta(days=1)
    start = datetime.combine(report_date, datetime.min.time())
    end = start + timedelta(days=1)

    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST)

    for dim in ["country", "genre"]:
        query = f"""
            SELECT
                '{report_date}' AS report_date,
                '{dim}'         AS dimension_type,
                {dim}           AS dimension_value,
                track_id,
                title,
                count()         AS total_plays
            FROM play_facts
            WHERE is_valid = 1
              AND event_timestamp >= toDateTime('{start}')
              AND event_timestamp <  toDateTime('{end}')
            GROUP BY {dim}, track_id, title
            ORDER BY total_plays DESC
            LIMIT 10
        """
        rows = client.execute(query)

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
            client.execute(
                "INSERT INTO daily_trending VALUES",
                data
            )

with DAG(
    dag_id="daily_trending_report",
    default_args=default_args,
    description="Compute daily Top 10 trending songs from ClickHouse",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    check_conn = PythonOperator(
        task_id="check_clickhouse_connection",
        python_callable=check_clickhouse_connection,
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
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
    Silver 層：從 raw_events（Bronze，原始心跳明細）精算出每個 session
    最終的 is_valid 結論，寫進 play_facts_historical。

    用 MERGE INTO（真正的 upsert）而不是 DELETE+INSERT，理由：
    - 這個任務本身要能安全地重跑（late data 抵達後重新計算同一天）
    - DELETE+INSERT 是兩個非原子操作，中間失敗會讓資料短暫消失
    - MERGE INTO 是單一原子操作，WHEN MATCHED 時直接覆蓋成最新結論，
      WHEN NOT MATCHED 時插入新結論，不會有「刪了但沒插入」的中間態
    """
    report_date = datetime.utcnow().date()
    start = datetime.combine(report_date, datetime.min.time())
    end = start + timedelta(days=1)

    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow")
    cursor = conn.cursor()

    # Silver 層 table，如果還不存在就建立
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

    # 對今天這個時間範圍內的所有 session，用 MAX(position_ms) >= 30000
    # 判定是否為有效播放，然後 upsert 進 play_facts_historical
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
    Gold 層：查 play_facts_historical（Silver，已精算過的 session 結論），
    算出當天各維度的 Top 10，寫進 ClickHouse daily_trending 給 API serving。
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
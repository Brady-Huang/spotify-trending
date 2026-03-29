import os
import json
import redis
import clickhouse_driver
from fastapi import FastAPI, Query
from typing import Literal

app = FastAPI(title="Spotify Trending Songs API")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
CACHE_TTL = 60  # 秒

def get_clickhouse_client():
    return clickhouse_driver.Client(host=CLICKHOUSE_HOST)

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

@app.get("/top_tracks")
def get_top_tracks(
    dim: Literal["country", "genre"] = Query(..., description="查詢維度"),
    num_tracks: int = Query(10, ge=1, le=100, description="回傳數量"),
    window: Literal["1h", "1d"] = Query("1h", description="時間窗口")
):
    cache_key = f"top_tracks:{dim}:{num_tracks}:{window}"

    # 1. 先查 Redis Cache
    r = get_redis_client()
    cached = r.get(cache_key)
    if cached:
        return {"source": "cache", "data": json.loads(cached)}

    # 2. Cache miss，查 ClickHouse
    client = get_clickhouse_client()

    if window == "1h":
        time_filter = "window_start >= toTimeZone(now(), 'Asia/Taipei') - INTERVAL 1 HOUR"
    else:
        time_filter = "window_start >= toTimeZone(now(), 'Asia/Taipei') - INTERVAL 1 DAY"

    query = f"""
        SELECT
            track_id,
            title,
            {dim} AS dimension,
            sum(play_count) AS total_plays
        FROM play_counts_1m
        WHERE {time_filter}
        GROUP BY track_id, title, {dim}
        ORDER BY total_plays DESC
        LIMIT {num_tracks}
    """

    rows = client.execute(query)
    result = [
        {
            "rank": i + 1,
            "track_id": r[0],
            "title": r[1],
            "dimension": r[2],
            "total_plays": r[3]
        }
        for i, r in enumerate(rows)
    ]

    # 3. 寫入 Cache
    r.set(cache_key, json.dumps(result), ex=CACHE_TTL)

    return {"source": "clickhouse", "data": result}

@app.get("/health")
def health():
    return {"status": "ok"}
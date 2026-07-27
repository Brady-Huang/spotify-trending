import os
import json
import redis
import clickhouse_driver
from fastapi import FastAPI, Query
from typing import Literal

app = FastAPI(title="Spotify Trending Songs API")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
CACHE_TTL = 60  # seconds

# Whitelist mapping from the validated `dim` value to the actual column name.
# Even if the Literal["country", "genre"] validation on the endpoint were ever
# loosened or removed, this lookup still only ever produces one of these two
# fixed strings — user input never reaches the SQL string directly.
DIM_COLUMN_MAP = {
    "country": "country",
    "genre": "genre",
}


def get_clickhouse_client():
    return clickhouse_driver.Client(host=CLICKHOUSE_HOST)

def get_redis_client():
    return redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

@app.get("/top_tracks")
def get_top_tracks(
    dim: Literal["country", "genre"] = Query(..., description="Dimension to group by"),
    num_tracks: int = Query(10, ge=1, le=100, description="Number of tracks to return"),
    window: Literal["1h", "1d"] = Query("1h", description="Time window")
):
    cache_key = f"top_tracks:{dim}:{num_tracks}:{window}"

    # 1. Check Redis cache first
    r = get_redis_client()
    cached = r.get(cache_key)
    if cached:
        return {"source": "cache", "data": json.loads(cached)}

    # 2. Cache miss — query ClickHouse
    client = get_clickhouse_client()

    if window == "1h":
        time_filter = "window_start >= now() - INTERVAL 1 HOUR"
    else:
        time_filter = "window_start >= now() - INTERVAL 1 DAY"

    # dim_column comes only from the whitelist map above, never from the raw
    # `dim` string, so it's safe to interpolate directly. num_tracks is passed
    # as a bound parameter rather than interpolated into the query string.
    dim_column = DIM_COLUMN_MAP[dim]

    query = f"""
        SELECT
            track_id,
            title,
            {dim_column} AS dimension,
            sum(play_count) AS total_plays
        FROM play_counts_1m
        WHERE {time_filter}
        GROUP BY track_id, title, {dim_column}
        ORDER BY total_plays DESC
        LIMIT %(num_tracks)s
    """

    rows = client.execute(query, {"num_tracks": num_tracks})
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

    # 3. Write result to cache
    r.set(cache_key, json.dumps(result), ex=CACHE_TTL)

    return {"source": "clickhouse", "data": result}

@app.get("/health")
def health():
    return {"status": "ok"}
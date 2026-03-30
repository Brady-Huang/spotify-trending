import json
import time
import uuid
import random
from kafka import KafkaProducer
from faker import Faker
import os

fake = Faker()

random.seed(42)     
Faker.seed(42)

KAFKA_TOPIC = "play-events"
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")



# 按真實用戶比例加權，模擬 hot shard 問題
# Spotify 2023 用戶分佈參考
COUNTRIES = [
    ("US", 0.28),   # 美國最大，容易成為 hot shard
    ("BR", 0.10),   # 巴西第二大市場
    ("UK", 0.08),
    ("DE", 0.06),
    ("MX", 0.06),
    ("FR", 0.05),
    ("AU", 0.04),
    ("CA", 0.04),
    ("JP", 0.04),
    ("KR", 0.03),
    ("TW", 0.02),   # 台灣佔比小
    ("OTHER", 0.20),
]

COUNTRY_NAMES = [c[0] for c in COUNTRIES]
COUNTRY_WEIGHTS = [c[1] for c in COUNTRIES]

# genre 也有流行度差異
GENRES = [
    ("pop", 0.35),
    ("hip-hop", 0.25),
    ("rock", 0.15),
    ("latin", 0.12),
    ("jazz", 0.07),
    ("classical", 0.06),
]

GENRE_NAMES = [g[0] for g in GENRES]
GENRE_WEIGHTS = [g[1] for g in GENRES]

# 重新產生 TRACKS，genre 按比例分配
TRACKS = [
    {
        "track_id": f"track_{i}",
        "title": fake.catch_phrase(),
        "genre": random.choices(GENRE_NAMES, weights=GENRE_WEIGHTS, k=1)[0]
    }
    for i in range(50)
]

random.seed()

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
def simulate_user_session(producer, user_id, track, country):
    """模擬一個用戶的完整收聽 session"""
    session_id = str(uuid.uuid4())
    position_ms = 0

    print(f"[Producer] User {user_id} | {country} | '{track['title']}' ({track['genre']})")

    will_complete = random.random() < 0.70
    heartbeat_count = 7 if will_complete else random.randint(1, 5)

    for i in range(heartbeat_count):
        state = "stop" if i == heartbeat_count - 1 else "play"

        event = {
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user_id,
            "track_id": track["track_id"],
            "title": track["title"],
            "genre": track["genre"],
            "country": country,
            "position_ms": position_ms,
            "state": state,
            "timestamp": time.time()
        }
        
        producer.send(KAFKA_TOPIC, value=event)

        # valid 只有在 stop 時才能判斷，播放中顯示 pending
        if state == "stop":
            is_valid = position_ms >= 30000
            print(f"  → state={state}, position_ms={position_ms}ms, valid={is_valid}")
        else:
            print(f"  → state={state}, position_ms={position_ms}ms, valid=pending")

        position_ms += 5000
        time.sleep(0.1)

def main():
    producer = create_producer()
    print(f"[Producer] Connected to Kafka: {KAFKA_BROKER}")
    print(f"[Producer] Topic: {KAFKA_TOPIC}")
    print(f"[Producer] Country distribution: US(28%) >> TW(2%)")

    user_ids = [f"user_{i}" for i in range(100)]

    while True:
        # 按加權比例選國家，模擬真實流量分佈
        user_id = random.choice(user_ids)
        track = random.choice(TRACKS)
        country = random.choices(COUNTRY_NAMES, weights=COUNTRY_WEIGHTS, k=1)[0]

        simulate_user_session(producer, user_id, track, country)
        time.sleep(0.3)

if __name__ == "__main__":
    main()
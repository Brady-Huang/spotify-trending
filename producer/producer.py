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



# Weighted distribution to simulate the hot shard problem.
# Approximate, illustrative weights based on Spotify's known market presence
# (US as largest market, decreasing shares for smaller markets) — not sourced
# from an official published breakdown.
COUNTRIES = [
    ("US", 0.28),   # Largest market, prone to becoming a hot shard
    ("BR", 0.10),   # Second largest market
    ("UK", 0.08),
    ("DE", 0.06),
    ("MX", 0.06),
    ("FR", 0.05),
    ("AU", 0.04),
    ("CA", 0.04),
    ("JP", 0.04),
    ("KR", 0.03),
    ("TW", 0.02),   # Small share
    ("OTHER", 0.20),
]

COUNTRY_NAMES = [c[0] for c in COUNTRIES]
COUNTRY_WEIGHTS = [c[1] for c in COUNTRIES]

# Genres also have differing popularity
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

# Generate TRACKS, with genre assigned by weighted distribution
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
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=5
    )
def simulate_user_session(producer, user_id, track, country):
    """Simulates one complete listening session for a user."""
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

        # is_valid can only be determined at "stop"; show "pending" while playing
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
        # Select country by weighted distribution to simulate real traffic patterns
        user_id = random.choice(user_ids)
        track = random.choice(TRACKS)
        country = random.choices(COUNTRY_NAMES, weights=COUNTRY_WEIGHTS, k=1)[0]

        simulate_user_session(producer, user_id, track, country)
        time.sleep(0.3)

if __name__ == "__main__":
    main()
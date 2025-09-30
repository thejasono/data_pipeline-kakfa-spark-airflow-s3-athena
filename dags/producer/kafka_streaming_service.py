"""
kafka_streaming_service.py

Tiny Kafka producer that fetches a user, normalizes it, and publishes to a topic
every PAUSE_INTERVAL seconds for STREAMING_DURATION seconds.
"""

import json
import time
import hashlib
import logging
import random
import uuid
from typing import Dict, Any, Optional, TYPE_CHECKING

import requests

LOGGER = logging.getLogger(__name__)

# Fallback data used if the API is unreachable.
FALLBACK_LOCATIONS: tuple[dict[str, str], ...] = (
    {"city": "London", "country": "United Kingdom", "street": "Baker Street", "postcode": "NW1 6XE", "latitude": "51.5237", "longitude": "-0.1585"},
    {"city": "Berlin", "country": "Germany", "street": "Unter den Linden", "postcode": "10117", "latitude": "52.5163", "longitude": "13.3777"},
    {"city": "Madrid", "country": "Spain", "street": "Gran Vía", "postcode": "28013", "latitude": "40.4203", "longitude": "-3.7058"},
    {"city": "Toronto", "country": "Canada", "street": "Queen Street West", "postcode": "M5V", "latitude": "43.6470", "longitude": "-79.3948"},
    {"city": "Sydney", "country": "Australia", "street": "George Street", "postcode": "2000", "latitude": "-33.8675", "longitude": "151.2070"},
)

FALLBACK_NAMES: dict[str, tuple[tuple[str, str], ...]] = {
    "male": (("Mr", "Noah"), ("Mr", "Liam"), ("Dr", "Elijah"), ("Mr", "Mateo")),
    "female": (("Ms", "Olivia"), ("Ms", "Emma"), ("Dr", "Ava"), ("Ms", "Sophia")),
}

FALLBACK_LAST_NAMES: tuple[str, ...] = ("Anderson", "Patel", "Kowalski", "Garcia", "Okafor", "Liu")


def _fallback_user_data() -> Dict[str, Any]:
    """Return a synthetic user when the API is unavailable."""
    rng = random.Random(time.time_ns())
    gender = rng.choice(tuple(FALLBACK_NAMES))
    title, first = rng.choice(FALLBACK_NAMES[gender])
    last = rng.choice(FALLBACK_LAST_NAMES)
    location = rng.choice(FALLBACK_LOCATIONS)
    street_number = rng.randint(1, 999)
    email_suffix = rng.randint(10, 99)

    return {
        "gender": gender,
        "name": {"title": title, "first": first, "last": last},
        "location": {
            "street": {"number": street_number, "name": location["street"]},
            "city": location["city"],
            "country": location["country"],
            "postcode": location["postcode"],
            "coordinates": {"latitude": location["latitude"], "longitude": location["longitude"]},
        },
        "email": f"{first.lower()}.{last.lower()}{email_suffix}@example.com",
        "login": {"uuid": f"offline-{uuid.uuid4()}"},
    }


if TYPE_CHECKING:  # Only for type-checkers.
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
else:
    Producer = Any


def _require_confluent_kafka() -> tuple[Any, Any, Any]:
    """Import confluent_kafka and raise a clear error if missing."""
    try:
        from confluent_kafka import Producer as _Producer
        from confluent_kafka.admin import AdminClient as _AdminClient, NewTopic as _NewTopic
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "confluent_kafka is required. Install it where this script runs (e.g., `pip install confluent-kafka`)."
        ) from exc
    return _Producer, _AdminClient, _NewTopic


# Connection targets:
# - Inside Docker network → "kafka:19092"
# - From host OS         → "localhost:9092"
KAFKA_BOOTSTRAP = "kafka:19092"
# KAFKA_BOOTSTRAP = "localhost:9092"

API_ENDPOINT = "https://randomuser.me/api/?results=1"
KAFKA_TOPIC = "names_topic"
PAUSE_INTERVAL = 10
STREAMING_DURATION = 120


# ---------------- Stage 1 — Setup (topic) ----------------
def ensure_topic(admin_bootstrap: str, topic: str, num_partitions: int = 1, replication_factor: int = 1) -> None:
    """Create the topic if missing; no-op if it already exists."""
    _, AdminClient, NewTopic = _require_confluent_kafka()
    admin = AdminClient({"bootstrap.servers": admin_bootstrap})

    md = admin.list_topics(timeout=5)
    if topic in md.topics:
        return

    futures = admin.create_topics([NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)])
    for t, f in futures.items():
        try:
            f.result()
            print(f"Created topic: {t}")
        except Exception as e:
            print(f"Topic create warning for {t}: {e}")


# ---------------- Stage 2 — Ingest (HTTP) ----------------
def retrieve_user_data(url: str = API_ENDPOINT, retries: int = 3, timeout: int = 10) -> Dict[str, Any]:
    """GET one random user; retry a few times; fall back to synthetic on repeated failure."""
    last_exception: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            return data["results"][0]
        except Exception as exc:
            last_exception = exc
            if attempt == retries:
                LOGGER.warning("Failed to reach %s after %s attempts; using fallback.", url, retries, exc_info=exc)
                break
            time.sleep(1)
    if last_exception is not None:
        return _fallback_user_data()
    raise RuntimeError("retrieve_user_data exited without data or exception")


# ---------------- Stage 3 — Transform ----------------
def safe_float(x: Any) -> Optional[float]:
    """float(x) or None on failure."""
    try:
        return float(x)
    except Exception:
        return None


def encrypt_zip(zip_code: Any) -> int:
    """Hash postcode → int (deterministic pseudonym)."""
    md5_hex = hashlib.md5(str(zip_code).encode("utf-8")).hexdigest()
    return int(md5_hex, 16)
    # For stronger privacy, prefer HMAC-SHA256 with a secret.


def transform_user_data(d: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten/normalize API payload; cast coords; hash postcode."""
    title = d["name"]["title"]
    first = d["name"]["first"]
    last = d["name"]["last"]
    name = f"{title}. {first} {last}"

    address = f"{d['location']['street']['number']}, {d['location']['street']['name']}"
    city = d["location"]["city"]
    nation = d["location"]["country"]
    postcode_hashed = str(encrypt_zip(d["location"]["postcode"]))
    lat = safe_float(d["location"]["coordinates"]["latitude"])
    lon = safe_float(d["location"]["coordinates"]["longitude"])
    email = d.get("email")

    return {
        "name": name,
        "gender": d.get("gender"),
        "address": address,
        "city": city,
        "nation": nation,
        "zip": postcode_hashed,
        "latitude": lat,
        "longitude": lon,
        "email": email,
    }


# ---------------- Stage 4 — Publish ----------------
def build_producer(bootstrap: str) -> Producer:
    """Build a Producer with idempotence, acks=all, light batching, and compression."""
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": "producer_instance",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "gzip",
        "linger.ms": 20,
        "batch.size": 64_000,
    }
    ProducerCls, _, _ = _require_confluent_kafka()
    return ProducerCls(conf)


def delivery_status(err, msg):
    """Per-message delivery report."""
    if err is not None:
        print("Delivery failed:", err)
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def publish_once(producer: Producer, topic: str, data: Dict[str, Any]) -> None:
    """Serialize dict → JSON bytes and enqueue to Kafka."""
    producer.produce(
        topic,
        value=json.dumps(data).encode("utf-8"),
        on_delivery=delivery_status,
    )
    producer.poll(0)  # Drive I/O and callbacks.


# ---------------- Orchestrator ----------------
def initiate_stream() -> None:
    """Run: ensure topic → build producer → loop(fetch→transform→produce) → flush."""
    ensure_topic(KAFKA_BOOTSTRAP, KAFKA_TOPIC, num_partitions=1, replication_factor=1)
    producer = build_producer(KAFKA_BOOTSTRAP)

    iterations = STREAMING_DURATION // PAUSE_INTERVAL
    try:
        for _ in range(iterations):
            raw = retrieve_user_data()
            payload = transform_user_data(raw)
            publish_once(producer, KAFKA_TOPIC, payload)
            time.sleep(PAUSE_INTERVAL)
    finally:
        remaining = producer.flush(10)
        if remaining:
            print(f"Flush timed out with {remaining} message(s) still in queue.")


if __name__ == "__main__":
    initiate_stream()

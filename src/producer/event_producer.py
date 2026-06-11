from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import UTC, datetime

from confluent_kafka import Producer

from src.producer.bad_events import BAD_EVENT_GENERATORS
from src.utils.logging_config import setup_logging

logger = logging.getLogger(__name__)
_running = True
EVENT_TYPES = ["purchase", "refund", "login", "logout", "click", "view", "signup"]
REGIONS = ["us-east", "us-west", "eu-central", "ap-south", "ap-northeast"]
DEVICE_TYPES = ["mobile", "desktop", "tablet", "tv", "wearable"]


def _handle_signal(signum: int, frame: object) -> None:
    global _running
    logger.info("Shutting down producer (signal %s)", signum)
    _running = False


def generate_event(sequence: int) -> dict:
    now = datetime.now(UTC)
    event_id = f"evt-{sequence:010d}-{uuid.uuid4().hex[:8]}"
    return {
        "event_id": event_id,
        "user_id": f"user_{random.randint(1, 50000):05d}",
        "account_id": f"acct_{random.randint(1, 10000):05d}",
        "event_type": random.choice(EVENT_TYPES),
        "amount": round(random.uniform(0.5, 999.99), 2),
        "event_timestamp": now.isoformat().replace("+00:00", "Z"),
        "ingest_timestamp": now.isoformat().replace("+00:00", "Z"),
        "region": random.choice(REGIONS),
        "device_type": random.choice(DEVICE_TYPES),
    }


def delivery_report(err, msg) -> None:
    if err:
        logger.error("Delivery failed: %s", err)


def main() -> None:
    setup_logging("event_producer")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    topic = os.getenv("KAFKA_TOPIC", "raw_events")
    eps = int(os.getenv("PRODUCER_EVENTS_PER_SECOND", "50"))
    batch_size = int(os.getenv("PRODUCER_BATCH_SIZE", "100"))
    bad_event_rate = float(os.getenv("BAD_EVENT_RATE", "0.02"))
    producer = Producer({"bootstrap.servers": bootstrap})
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    logger.info(
        "Publishing to %s @ %s (~%d events/sec, bad_event_rate=%.2f)",
        topic,
        bootstrap,
        eps,
        bad_event_rate,
    )
    sequence = 0
    interval = 1.0 / max(eps, 1)
    while _running:
        batch_start = time.monotonic()
        for _ in range(batch_size):
            if not _running:
                break
            sequence += 1
            if bad_event_rate > 0 and random.random() < bad_event_rate:
                raw_payload = random.choice(BAD_EVENT_GENERATORS)()
                producer.produce(topic, value=raw_payload.encode("utf-8"), callback=delivery_report)
            else:
                event = generate_event(sequence)
                producer.produce(
                    topic,
                    key=event["event_id"].encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )
        producer.poll(0)
        elapsed = time.monotonic() - batch_start
        target = batch_size * interval
        if elapsed < target:
            time.sleep(target - elapsed)
        if sequence % 1000 == 0:
            logger.info("Published %d events", sequence)
    producer.flush(10)
    logger.info("Producer stopped after %d events", sequence)


if __name__ == "__main__":
    main()

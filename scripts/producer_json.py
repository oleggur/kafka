#!/usr/bin/env python3
"""
Day 2 - Phase A: Minimal JSON Producer (typed)
- Connects to Kafka at localhost:9092
- Writes JSON-encoded lines to topic 'ads_raw_json'
- Value is a UTF-8 JSON string (no schema registry)
"""

from __future__ import annotations

import json  # standard lib for JSON encoding
import time  # small sleeps so output is easier to read
from typing import Any, Dict, Optional

from confluent_kafka import Producer, Message  # Kafka producer client + message type
from confluent_kafka import KafkaError  # error type for delivery report


# ---- Producer configuration ----
# 'bootstrap.servers' = how to reach the Kafka cluster (host:port).
# We exposed HOST listener at localhost:9092 in docker-compose.yml
producer_conf: Dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    # 'linger.ms' can batch small messages briefly; keep it small for demo responsiveness
    "linger.ms": 5,
    # 'enable.idempotence' off by default; leave it off for simple demo
}


# Construct the single Producer instance (thread-safe for sending from one process)
producer: Producer = Producer(producer_conf)


def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """
    Delivery callback: Kafka calls this when the broker acks the message (or on error).
    - err is an exception-like KafkaError if delivery failed, else None
    - msg contains metadata like topic, partition, offset
    """
    if err is not None:
        print(f"[DELIVERY-ERROR] {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    topic: str = "ads_raw_json"  # send to this topic; auto-created if missing (since we enabled auto-create)

    # We'll emit a few example "ad events" (fake numbers), JSON-encoded.
    events: list[Dict[str, Any]] = [
        {"campaign_id": "123", "spend": 10.5, "currency": "USD"},
        {"campaign_id": "123", "spend": 7.25, "currency": "USD"},
        {"campaign_id": "456", "spend": 3.10, "currency": "USD"},
        {"campaign_id": "789", "spend": 42.0, "currency": "USD"},
    ]

    for i, event in enumerate(events, start=1):
        # Serialize dict -> JSON string -> bytes (utf-8)
        payload_str: str = json.dumps(event)
        payload_bytes: bytes = payload_str.encode("utf-8")

        # (optional) set a key to control partitioning; hash by campaign_id so same campaign hits same partition
        key_bytes: bytes = event["campaign_id"].encode("utf-8")

        # Asynchronously send one message. Kafka client will batch/flush under the hood.
        # - topic: destination topic
        # - value: message bytes
        # - key:   optional key bytes (used for partitioning; messages with same key go to same partition)
        # - on_delivery: callback so we can see when it was acked
        producer.produce(
            topic=topic,
            value=payload_bytes,
            key=key_bytes,
            on_delivery=delivery_report,
        )

        # Serve delivery callbacks from background queue (acks, errors)
        producer.poll(0)

        print(f"[SENT] #{i}: {payload_str}")
        time.sleep(0.25)  # slow down a bit for readability

    # Ensure all buffered messages are actually sent before exiting
    producer.flush(timeout=10)
    print("[FLUSHED] all messages delivered (or timed out).")


if __name__ == "__main__":
    main()

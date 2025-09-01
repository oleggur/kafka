#!/usr/bin/env python3
"""
Day 2 - Phase A: Minimal JSON Consumer (typed)
- Joins a consumer group 'g_json_demo'
- Subscribes to 'ads_raw_json'
- Prints messages as they arrive (value decoded from UTF-8)
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message


# ---- Consumer configuration ----
# - 'bootstrap.servers': how to reach Kafka
# - 'group.id':          name of your consumer group (enables offset tracking + rebalancing)
# - 'auto.offset.reset': where to start if no committed offsets exist for this group:
#                        'earliest' = from beginning; 'latest' = only new messages
consumer_conf: Dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "g_json_demo",
    "auto.offset.reset": "earliest",
    # leave commits as auto (default); we'll keep it simple on Day 2
}

consumer: Consumer = Consumer(consumer_conf)


def main() -> None:
    topic: str = "ads_raw_json"
    # Subscribe assigns partitions dynamically and participates in rebalances
    consumer.subscribe([topic])

    print("[CONSUMER] waiting for messages… Ctrl+C to stop.")
    try:
        while True:
            # poll() returns a Message or None if no message within timeout
            msg: Optional[Message] = consumer.poll(timeout=1.0)
            if msg is None:
                continue  # no data this second; loop again
            if msg.error():
                # .error() returns a KafkaError for non-data events (EOF, etc.)
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # not an error: just end of partition for now
                    continue
                raise KafkaException(msg.error())

            # Decode value bytes -> JSON
            raw: Optional[bytes] = msg.value()
            value: Any = json.loads(raw.decode("utf-8")) if raw is not None else None

            # Key is optional; decode if present
            raw_key: Optional[bytes] = msg.key()
            key: Optional[str] = raw_key.decode("utf-8") if raw_key else None

            print(
                f"[RECV] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} "
                f"key={key} value={value}"
            )
    except KeyboardInterrupt:
        print("\n[CONSUMER] stopping…")
    finally:
        # Close triggers a final commit (if enabled) and leaves the group cleanly
        consumer.close()
        print("[CONSUMER] closed.")


if __name__ == "__main__":
    main()

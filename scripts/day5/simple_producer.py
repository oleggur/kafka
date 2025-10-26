"""
Simple Kafka producer that generates synthetic ad spend events
and sends them into a Kafka topic.

Schema of each event:
  - seq (int)          → running sequence number
  - campaign_id (str)  → fake campaign ID
  - spend (float)      → random ad spend
  - currency (str)     → always "USD" here

These fields are JSON-encoded and published to Kafka.
"""

import json
import random
import time
from confluent_kafka import Producer


def delivery_report(err, msg) -> None:
    """
    Callback executed when Kafka acknowledges a message.
    Helps debug delivery problems.
    """
    if err is not None:
        print(f"[ERROR] delivery failed: {err}")
    else:
        print(f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def main() -> None:
    # Configure producer connection
    # bootstrap.servers → where Kafka broker is running (inside Docker compose: localhost:9092)
    conf: dict[str, str] = {"bootstrap.servers": "localhost:9092"}

    producer: Producer = Producer(conf)

    topic: str = "ads_simple"

    print(f"[START] producing to topic '{topic}'...")

    for seq in range(10):  # send 10 messages
        event: dict[str, object] = {
            "seq": seq,
            "campaign_id": f"c{seq % 3}",  # rotate between c0, c1, c2
            "spend": round(random.random() * 100, 2),
            "currency": "USD",
        }

        key: str = event["campaign_id"]  # use campaign_id as key (so same campaign always hashes to same partition)
        value: str = json.dumps(event)

        # Send the message asynchronously
        producer.produce(
            topic=topic,
            key=key,
            value=value,
            on_delivery=delivery_report,  # optional callback
        )

        # poll(0) → allows producer to handle delivery callbacks
        # could also be poll(1) to wait up to 1s if you want blocking
        producer.poll(0)

        time.sleep(0.2)

    # Ensure all buffered messages are sent
    producer.flush()

    print("[DONE] produced 10 events.")


if __name__ == "__main__":
    main()

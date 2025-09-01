#!/usr/bin/env python3
"""
Day 3 — Producer partitioning demo (fully typed + heavily commented)

What this script demonstrates:
1) Sending messages **without a key** → Kafka's default partitioner spreads them
   round-robin across partitions (good for balancing throughput).
2) Sending messages **with a key** (e.g., campaign_id) → all messages with the same key
   are routed to the **same partition** (preserves per-key order).

You will see which partition each message landed on via the delivery callback output.

Run (from repo root, with your venv activated):
    python scripts/producer_partitioned.py
    # optional flags:
    #   --topic ads_partitioned     (topic name; default "ads_partitioned")
    #   --unkeyed 6                 (# of unkeyed messages to send first; default 6)
    #   --keyed 6                   (# of keyed messages to send after; default 6)
    #   --sleep 0.1                 (seconds to sleep between sends; default 0.15)

Tip:
  Start one or more consumers that print the partition for each message:
    docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --topic ads_partitioned \
      --group g3_demo \
      --bootstrap-server localhost:9092 \
      --property print.partition=true
"""

from __future__ import annotations

import argparse
import time
from typing import Any, Dict, Optional

from confluent_kafka import Producer, KafkaError, Message


def build_args() -> argparse.Namespace:
    """
    Parse CLI flags for the script.

    Returns:
        argparse.Namespace: object with attributes:
            topic (str):           destination topic name
            unkeyed (int):         # of unkeyed messages to send
            keyed (int):           # of keyed messages to send
            sleep (float):         seconds to sleep between sends
    """
    parser = argparse.ArgumentParser(
        description="Kafka partitioning demo: send unkeyed + keyed messages and show assigned partitions."
    )
    parser.add_argument(
        "--topic",
        type=str,
        default="ads_partitioned",
        help="Destination topic (default: ads_partitioned). "
             "Create it up-front with partitions=3 to see clear distribution.",
    )
    parser.add_argument(
        "--unkeyed",
        type=int,
        default=6,
        help="Number of UNKEYED messages to send first (default: 6). "
             "Unkeyed messages are balanced round-robin across partitions.",
    )
    parser.add_argument(
        "--keyed",
        type=int,
        default=6,
        help="Number of KEYED messages to send after (default: 6). "
             "Keyed messages use the key's hash for partitioning; same key -> same partition.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.15,
        help="Sleep between sends in seconds (default: 0.15) to make logs readable.",
    )
    return parser.parse_args()


def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """
    Producer delivery callback:
    - Called once the broker acknowledges a message (or if delivery fails).
    - Useful for logging success/failure and seeing the actual partition chosen.

    Args:
        err: KafkaError if delivery failed, else None on success.
        msg: The produced Message with metadata (topic, partition, offset, timestamp).
    """
    if err is not None:
        # If you see errors here, they’re typically connectivity, authorization, or topic issues.
        print(f"[DELIVERY-ERROR] topic={msg.topic()} key={msg.key()} err={err}")
        return

    # On success, Kafka tells us the actual partition + offset chosen.
    # - For UNKEYED messages: expect roughly round-robin across partitions.
    # - For KEYED messages: all identical keys will stick to the same partition.
    key_str: Optional[str] = msg.key().decode("utf-8") if msg.key() else None
    print(
        f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key_str}"
    )


def make_producer() -> Producer:
    """
    Build a high-throughput async Kafka Producer.

    Returns:
        Producer: Confluent Kafka producer (librdkafka under the hood).
    """
    # Producer config (minimal set for local dev)
    # - bootstrap.servers: how to reach the cluster. We expose HOST listener at localhost:9092.
    # - linger.ms: small batching window; balances latency vs throughput for tiny messages.
    # - enable.idempotence: keeping default (False) for simplicity today. We'll discuss later in exactly-once.
    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,
    }
    return Producer(conf)


def send_unkeyed_messages(producer: Producer, topic: str, count: int, pause: float) -> None:
    """
    Send messages without keys. Kafka will distribute them round-robin across partitions.

    Args:
        producer: The shared Producer instance.
        topic:    Destination topic.
        count:    How many unkeyed messages to send.
        pause:    Seconds to sleep between sends (for readability).
    """
    for i in range(count):
        # No key → partitioner defaults to round-robin (within the producer session).
        value_str: str = f'{{"type":"unkeyed","seq":{i}}}'
        value_bytes: bytes = value_str.encode("utf-8")

        # Asynchronous send:
        # - topic: destination topic
        # - value: payload bytes
        # - key:   omitted (None) -> unkeyed
        # - on_delivery: callback so we see the chosen partition after ack
        producer.produce(topic=topic, value=value_bytes, on_delivery=delivery_report)

        # Pump the producer event loop (serves delivery callbacks, handles retries, etc.)
        # timeout=0 → non-blocking; just process anything that’s already ready
        producer.poll(0)

        print(f"[SENT-UNKEYED] #{i} value={value_str}")
        time.sleep(pause)


def send_keyed_messages(producer: Producer, topic: str, count: int, pause: float) -> None:
    """
    Send messages with semantically meaningful keys (e.g., campaign_id).
    Messages with the same key are guaranteed to go to the SAME partition,
    preserving order for that key.

    Args:
        producer: The shared Producer instance.
        topic:    Destination topic.
        count:    How many keyed messages to send in total.
        pause:    Seconds to sleep between sends (for readability).
    """
    # We'll alternate among 3 campaign keys to show stable partitioning per key.
    keys: list[str] = ["campaign:123", "campaign:456", "campaign:789"]

    for i in range(count):
        key_str: str = keys[i % len(keys)]            # cycle keys 123 -> 456 -> 789 -> 123 ...
        key_bytes: bytes = key_str.encode("utf-8")    # Kafka keys are bytes
        value_str: str = f'{{"type":"keyed","seq":{i},"campaign":"{key_str}"}}'
        value_bytes: bytes = value_str.encode("utf-8")

        # Produce with a key:
        # - The default partitioner hashes the key (murmur2) to pick a partition.
        # - All messages with identical key bytes map to the same partition (stable within a partition count).
        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=delivery_report,
        )

        producer.poll(0)  # process delivery callbacks, etc.
        print(f"[SENT-KEYED]   #{i} key={key_str} value={value_str}")
        time.sleep(pause)


def main() -> None:
    """
    Entry point:
    - Ensures the topic exists (create it beforehand with partitions=3 for clarity).
    - Sends a batch of UNKEYED messages, then a batch of KEYED messages.
    - Flushes the producer to guarantee delivery before exiting.
    """
    args = build_args()

    # 1) Build a single Producer instance (thread-safe for sending in one process)
    producer: Producer = make_producer()

    print(
        f"[START] topic='{args.topic}', unkeyed={args.unkeyed}, keyed={args.keyed}, sleep={args.sleep}s\n"
        f"        Expect UNKEYED to round-robin partitions, KEYED to stick per-key."
    )

    # 2) Send unkeyed messages (round-robin across partitions)
    send_unkeyed_messages(producer, args.topic, args.unkeyed, args.sleep)

    # 3) Send keyed messages (stable partition per key)
    send_keyed_messages(producer, args.topic, args.keyed, args.sleep)

    # 4) Ensure all enqueued messages are delivered (or time out)
    #    - flush() internally polls until outstanding messages are 0 or timeout elapses.
    outstanding: int = producer.flush(timeout=10.0)
    if outstanding == 0:
        print("[FLUSHED] all messages delivered.")
    else:
        # If >0 remain, they failed to deliver within the timeout (e.g., broker down).
        print(f"[FLUSHED] WARNING: {outstanding} messages not delivered before timeout.")


if __name__ == "__main__":
    main()

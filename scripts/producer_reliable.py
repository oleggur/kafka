#!/usr/bin/env python3
"""
Day 4 — Reliability-tuned Producer (fully typed + heavily commented)

What this shows:
- Safe producer settings for durability and duplicate-prevention on retries:
    * acks=all                  → wait for leader + in-sync replicas (ISR)
    * enable.idempotence=True   → broker de-dupes producer retries within a session
    * linger.ms / batch.size    → micro-batching knobs (keep modest for demos)
    * delivery.timeout.ms       → upper bound for retrying before reporting failure
    * max.in.flight.requests.per.connection=5 (default) → compatible with idempotence

Notes:
- Idempotence prevents duplicates caused by the *producer's own retries in the same
  producer session*. It does NOT dedupe if **your app** re-sends the same record after
  a restart. For end-to-end “exactly-once”, you need **transactions** (later today).
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, Optional

from confluent_kafka import KafkaError, Message, Producer


def build_args() -> argparse.Namespace:
    """
    CLI flags so you can vary topic, throughput and payload shape.

    Returns:
        argparse.Namespace with:
            topic (str): destination topic
            count (int): number of messages to send
            sleep (float): seconds to sleep between sends (readability)
            keyed (bool): whether to send a deterministic key (keeps per-key order)
    """
    ap = argparse.ArgumentParser(description="Reliable Kafka producer with acks=all + idempotence.")
    ap.add_argument("--topic", type=str, default="ads_reliable",
                    help="Destination topic (default: ads_reliable).")
    ap.add_argument("--count", type=int, default=12,
                    help="How many messages to send (default: 12).")
    ap.add_argument("--sleep", type=float, default=0.10,
                    help="Sleep between sends in seconds (default: 0.10).")
    ap.add_argument("--keyed", action="store_true",
                    help="If set, send a key 'campaign:123' so all messages go to one partition.")
    return ap.parse_args()


def make_producer() -> Producer:
    """
    Build a reliability-tuned Producer.

    Returns:
        Producer: Confluent Kafka producer (async, high-throughput).
    """
    conf: Dict[str, Any] = {
        # How to reach your broker from the HOST (see docker-compose HOST listener)
        "bootstrap.servers": "localhost:9092",

        # ---- SAFETY / RELIABILITY ----
        # Wait for leader + all in-sync replicas → highest durability (at small latency cost).
        "acks": "all",

        # Turn on idempotence → retries won’t create duplicates within this producer session.
        "enable.idempotence": True,

        # Retry budget is governed by delivery.timeout.ms (not a raw "retries" count in librdkafka).
        # The producer will keep retrying retriable errors (e.g., leader change) until this timeout.
        "delivery.timeout.ms": 120_000,   # 2 minutes upper bound for retries

        # Keep in-flight requests per connection at 5 (default) → compatible with idempotence.
        # (If you manually change this, keep it <=5 with idempotence.)
        "max.in.flight.requests.per.connection": 5,

        # ---- BATCHING / LATENCY ----
        # Small linger creates tiny micro-batches (better throughput than 0, still responsive).
        "linger.ms": 5,
        # Optional: cap batch size (bytes). Leave default unless you want to experiment.
        # "batch.size": 64_000,
    }
    return Producer(conf)


def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """
    Called once per produced message when the broker acks (or on failure).

    With idempotence + acks=all:
    - On transient errors the client transparently retries; you still get exactly one success
      callback (or a final failure after delivery.timeout.ms).
    """
    key_str: Optional[str] = msg.key().decode("utf-8") if msg.key() else None
    if err is not None:
        print(f"[DELIVERY-ERROR] topic={msg.topic()} key={key_str} err={err}")
        return
    print(f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} key={key_str}")


def main() -> None:
    args = build_args()
    producer: Producer = make_producer()

    print(
        f"[START] reliable producer → topic='{args.topic}', count={args.count}, "
        f"keyed={args.keyed}, sleep={args.sleep}s\n"
        f"        Using acks=all + enable.idempotence=True (safe retries, no dupes within session)."
    )

    for i in range(args.count):
        # Build a small JSON payload (text/binary is fine; Kafka only cares about bytes)
        event = {"type": "reliable", "seq": i, "ts": time.time()}
        value_bytes: bytes = json.dumps(event).encode("utf-8")

        # Optional key: fixes messages to a single partition and preserves per-key order
        key_bytes: Optional[bytes] = b"campaign:123" if args.keyed else None

        # Async send; the library will batch + retry as needed.
        # on_delivery lets us observe partitions/offsets and any final failures.
        producer.produce(
            topic=args.topic,
            value=value_bytes,
            key=key_bytes,
            on_delivery=delivery_report,
        )

        # Drive the producer event loop (serve delivery callbacks, handle retries, etc.)
        producer.poll(0)

        print(f"[SENT] #{i} key={key_bytes.decode('utf-8') if key_bytes else None} value={event}")
        time.sleep(args.sleep)

    # Ensure all buffered messages are delivered (or give up at timeout)
    # Returns the number of undelivered messages (0 on success).
    remaining: int = producer.flush(timeout=15.0)
    if remaining == 0:
        print("[FLUSHED] all messages delivered.")
    else:
        print(f"[FLUSHED] WARNING: {remaining} messages not delivered before timeout.")


if __name__ == "__main__":
    main()

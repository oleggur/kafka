#!/usr/bin/env python3
"""
Tutorial 04-A: Idempotent Producer
==================================

This script demonstrates:
- Idempotent producers for exactly-once semantics
- How retries can cause duplicates without idempotence
- Producer acknowledgment levels (acks)
- Retry configuration

Key Concepts:
-------------
1. IDEMPOTENCE: Producing the same message multiple times has the same effect as once
   - Prevents duplicates even if producer retries
   - Kafka assigns sequence numbers to detect duplicates
   - Enabled with enable.idempotence=true

2. PRODUCER RETRIES: What happens when message send fails
   - Network errors, broker failures, timeouts
   - Without idempotence: retries can create duplicates
   - With idempotence: duplicates are detected and dropped

3. ACKNOWLEDGMENTS (ACKS): How many replicas must ack before success
   - acks=0: No acknowledgment (fire-and-forget, fastest, least reliable)
   - acks=1: Wait for leader replica only (fast, might lose data if leader crashes)
   - acks=all: Wait for all in-sync replicas (slowest, most reliable)

4. IN-SYNC REPLICAS (ISR): Replicas that are caught up with the leader
   - Leader tracks which replicas are "in sync"
   - acks=all waits for all ISR replicas (not all replicas)

How Idempotence Works:
----------------------
1. Producer assigns sequence numbers to messages (per partition)
2. Kafka broker tracks sequence numbers
3. If broker receives duplicate (same sequence #), it drops the duplicate
4. Broker acknowledges both original and duplicate (producer doesn't retry)

When to Use Idempotence:
------------------------
- ALWAYS in production (no downside with modern Kafka)
- Small performance cost (sequence number overhead)
- Prevents duplicates from retries
- Required for transactions (Tutorial 05)
"""

import json
import random
import time
from typing import Dict, Any, Optional

from confluent_kafka import Producer, KafkaError


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """Callback to track delivery status."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[DELIVERED] partition={msg.partition()} offset={msg.offset()}"
        )


def demo_non_idempotent() -> None:
    """
    Demonstrate non-idempotent producer (can create duplicates).
    """
    print("=" * 60)
    print("DEMO 1: Non-Idempotent Producer (Default)")
    print("=" * 60)
    print("Without idempotence, retries can cause duplicate messages")
    print()

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",

        # IDEMPOTENCE DISABLED (default in older versions)
        "enable.idempotence": False,

        # ACKNOWLEDGMENTS: How many replicas must ack
        # acks=1: Only the leader replica must ack (default)
        # This is faster but less reliable than acks=all
        "acks": "1",

        # RETRIES: How many times to retry if send fails
        # Network glitches, broker restarts, etc. can cause retries
        "retries": 3,

        # RETRY.BACKOFF.MS: Wait time between retries
        "retry.backoff.ms": 100,
    }

    producer: Producer = Producer(producer_config)
    topic: str = "ads_reliability_nonidem"

    print("[CONFIG] Idempotence: DISABLED")
    print(f"[CONFIG] Acknowledgments: {producer_config['acks']}")
    print(f"[CONFIG] Retries: {producer_config['retries']}")
    print()

    # Send 10 messages (enough to demonstrate auto-commit data loss)
    for seq in range(10):
        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": f"campaign_{seq}",
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "idempotent": False
        }

        key: str = event["campaign_id"]
        value: str = json.dumps(event)

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            on_delivery=delivery_report
        )

        producer.poll(0)
        print(f"[SENT] seq={seq}")
        time.sleep(0.1)

    producer.flush()

    print()
    print("[RISK] If network errors cause retries, messages may be duplicated!")
    print("[RISK] Consumer might see the same message multiple times")
    print()


def demo_idempotent() -> None:
    """
    Demonstrate idempotent producer (prevents duplicates).
    """
    print("=" * 60)
    print("DEMO 2: Idempotent Producer")
    print("=" * 60)
    print("With idempotence, retries will NOT cause duplicates")
    print()

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",

        # IDEMPOTENCE ENABLED
        # Kafka Concept - EXACTLY-ONCE SEMANTICS:
        #   Producer assigns sequence numbers to messages
        #   Broker detects and drops duplicates based on sequence numbers
        #   This ensures each message is written exactly once per partition
        "enable.idempotence": True,

        # When idempotence is enabled, these settings are automatically enforced:
        # - acks=all (wait for all in-sync replicas)
        # - retries=MAX_INT (retry indefinitely until success)
        # - max.in.flight.requests.per.connection <= 5 (limit concurrent requests)

        # We can still override retries if we want a limit
        "retries": 3,
    }

    producer: Producer = Producer(producer_config)
    topic: str = "ads_reliability_idem"

    print("[CONFIG] Idempotence: ENABLED")
    print("[CONFIG] Acknowledgments: all (enforced by idempotence)")
    print("[CONFIG] Retries: 3")
    print()

    # Send 10 messages (enough to demonstrate auto-commit data loss)
    for seq in range(10):
        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": f"campaign_{seq}",
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "idempotent": True
        }

        key: str = event["campaign_id"]
        value: str = json.dumps(event)

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            on_delivery=delivery_report
        )

        producer.poll(0)
        print(f"[SENT] seq={seq}")
        time.sleep(0.1)

    producer.flush()

    print()
    print("[GUARANTEE] Even if retries occur, messages will NOT be duplicated!")
    print("[GUARANTEE] Each message written exactly once to each partition")
    print()


def demo_acks_levels() -> None:
    """
    Demonstrate different acknowledgment levels.
    """
    print("=" * 60)
    print("DEMO 3: Acknowledgment Levels")
    print("=" * 60)
    print()

    # Dictionary of acks levels and their trade-offs
    acks_levels = {
        "0": {
            "desc": "No acknowledgment (fire-and-forget)",
            "reliability": "Lowest - messages may be lost",
            "performance": "Fastest",
            "use_case": "Metrics, logs where some loss is acceptable"
        },
        "1": {
            "desc": "Leader acknowledgment only",
            "reliability": "Medium - messages may be lost if leader crashes",
            "performance": "Fast",
            "use_case": "Default for most use cases"
        },
        "all": {
            "desc": "All in-sync replicas acknowledge",
            "reliability": "Highest - messages not lost unless all ISR fail",
            "performance": "Slower (waits for replication)",
            "use_case": "Critical data (payments, orders)"
        }
    }

    for acks, info in acks_levels.items():
        print(f"acks={acks}: {info['desc']}")
        print(f"  Reliability: {info['reliability']}")
        print(f"  Performance: {info['performance']}")
        print(f"  Use case: {info['use_case']}")
        print()


def main() -> None:
    """
    Run all reliability demos.
    """

    print("\n" + "=" * 60)
    print("KAFKA PRODUCER RELIABILITY TUTORIAL")
    print("=" * 60)
    print()

    # Demo 1: Non-idempotent (can create duplicates)
    demo_non_idempotent()
    time.sleep(1)

    # Demo 2: Idempotent (prevents duplicates)
    demo_idempotent()
    time.sleep(1)

    # Demo 3: Acks levels
    demo_acks_levels()

    # Best practices summary
    print("=" * 60)
    print("BEST PRACTICES")
    print("=" * 60)
    print("""
1. ALWAYS enable idempotence in production:
   - enable.idempotence=true
   - No significant performance cost
   - Prevents duplicates from retries

2. Choose acks based on reliability requirements:
   - Critical data: acks=all (idempotence enforces this)
   - Normal data: acks=1 (default)
   - Lossy data: acks=0 (rare, only for metrics/logs)

3. Set appropriate retry configuration:
   - retries=MAX_INT or high number (let Kafka retry)
   - retry.backoff.ms=100 (wait between retries)
   - delivery.timeout.ms=120000 (total time for delivery)

4. Monitor delivery failures:
   - Always use delivery callbacks
   - Log failures to alerting system
   - Have dead letter queue for failed messages

5. For exactly-once semantics:
   - enable.idempotence=true (Producer)
   - Transactional producer (Tutorial 05)
   - Read committed isolation (Consumer)
    """)


if __name__ == "__main__":
    main()

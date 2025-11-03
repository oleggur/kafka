#!/usr/bin/env python3
"""
Tutorial 05: Kafka Transactions (Exactly-Once Semantics)
========================================================

This script demonstrates:
- Transactional producer for exactly-once semantics (EOS)
- Atomic writes across multiple partitions/topics
- Read-process-write pattern with transactions
- Transaction abort and commit

Key Concepts:
-------------
1. TRANSACTIONS: Group multiple messages into atomic unit
   - All messages commit together or none do
   - Works across multiple topics and partitions
   - Enables exactly-once semantics (EOS)

2. EXACTLY-ONCE SEMANTICS (EOS):
   - Producer: Idempotent + Transactional
   - Consumer: read_committed isolation level
   - Result: Each message processed exactly once end-to-end

3. TRANSACTIONAL.ID: Unique identifier for transactional producer
   - Must be unique per producer instance
   - Enables fencing (prevents zombie producers)
   - Kafka tracks state per transactional.id

4. TRANSACTION LIFECYCLE:
   - init_transactions(): Initialize transactional state
   - begin_transaction(): Start a new transaction
   - produce(): Send messages (buffered until commit)
   - commit_transaction(): Make messages visible to consumers
   - abort_transaction(): Discard messages (rollback)

5. FENCING: Preventing zombie producers
   - Kafka uses epoch numbers to identify latest producer
   - Old producers (zombies) are automatically fenced out
   - Prevents duplicate writes from old instances

Use Cases:
----------
1. READ-PROCESS-WRITE: Consume from one topic, produce to another
2. AGGREGATION: Read multiple messages, write summary atomically
3. MULTI-TOPIC WRITES: Write to multiple topics atomically
4. DEDUPLICATION: Ensure each message processed exactly once

Example: ETL Pipeline
--------------------
Input Topic: raw_events
Output Topic: processed_events

Without Transactions:
1. Read event from raw_events
2. Process event
3. Write to processed_events
4. Commit offset for raw_events
Problem: If crash between steps 3-4, event is lost or duplicated!

With Transactions:
1. Begin transaction
2. Read event from raw_events
3. Process event
4. Write to processed_events
5. Commit offset for raw_events (part of transaction)
6. Commit transaction
Result: Either all succeed or all fail (atomic)
"""

import time
import json
import random
from typing import Dict, Any

from confluent_kafka import Producer, KafkaException


def main() -> None:
    """
    Demonstrate transactional producer.
    """

    # ============================================================================
    # TRANSACTIONAL PRODUCER CONFIGURATION
    # ============================================================================

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",

        # TRANSACTIONAL.ID: Unique identifier for this producer
        # Kafka Concept - TRANSACTIONAL ID:
        #   - Must be unique per producer instance
        #   - Kafka uses this to track transaction state
        #   - Enables fencing (prevents zombie producers)
        #   - If you restart producer with same ID, it resumes/aborts previous transaction
        "transactional.id": "txn-producer-tutorial-1",

        # Idempotence is automatically enabled for transactional producers
        # (can't have transactions without idempotence)
        "enable.idempotence": True,

        # Transactions enforce these settings:
        # - acks=all (wait for all in-sync replicas)
        # - max.in.flight.requests <= 5
    }

    producer: Producer = Producer(producer_config)

    print("=" * 60)
    print("KAFKA TRANSACTIONS TUTORIAL")
    print("=" * 60)
    print(f"Transactional ID: {producer_config['transactional.id']}")
    print("-" * 60)
    print()

    # ============================================================================
    # INITIALIZE TRANSACTIONS
    # ============================================================================
    # Kafka Concept - TRANSACTION INITIALIZATION:
    #   Must be called once before any transactions
    #   - Registers transactional.id with Kafka
    #   - Recovers or aborts any incomplete transactions from previous run
    #   - Prepares producer for transactional writes

    print("[INIT] Initializing transactions...")
    producer.init_transactions()
    print("[INIT] Transactions initialized successfully")
    print()

    # Topics for demo
    topic_output: str = "ads_transactions_output"

    # ============================================================================
    # DEMO 1: Simple Transaction (Single Topic)
    # ============================================================================
    print("-" * 60)
    print("DEMO 1: Simple Transaction")
    print("-" * 60)
    print("Writing multiple messages atomically to one topic")
    print()

    try:
        # BEGIN TRANSACTION
        # All produce() calls after this are part of the transaction
        # Messages are buffered and not visible to consumers yet
        producer.begin_transaction()
        print("[TXN] Transaction started")

        # Send multiple messages within transaction
        for seq in range(3):
            event: Dict[str, Any] = {
                "seq": seq,
                "campaign_id": f"campaign_{seq}",
                "spend": round(random.uniform(10.0, 100.0), 2),
                "currency": "USD",
                "transaction": "demo1"
            }

            key: str = event["campaign_id"]
            value: str = json.dumps(event)

            # PRODUCE (buffered, not yet committed)
            producer.produce(
                topic=topic_output,
                key=key.encode("utf-8"),
                value=value.encode("utf-8")
            )

            # Must call poll() to handle delivery callbacks
            producer.poll(0)
            print(f"[TXN] Produced seq={seq} (buffered)")
            time.sleep(4)

        # COMMIT TRANSACTION
        # Kafka Concept - ATOMIC COMMIT:
        #   All messages become visible to consumers atomically
        #   Either all messages are visible or none are
        #   This is a durable commit (survives broker failures)
        print("[TXN] Committing transaction...")
        producer.commit_transaction()
        print("[TXN] Transaction committed successfully!")
        print("[TXN] All 3 messages are now visible to consumers")
        print()

    except KafkaException as e:
        # If anything goes wrong, abort the transaction
        print(f"[TXN ERROR] {e}")
        print("[TXN] Aborting transaction...")
        producer.abort_transaction()
        print("[TXN] Transaction aborted (messages discarded)")

    # ============================================================================
    # DEMO 2: Transaction with Abort (Error Handling)
    # ============================================================================
    print("-" * 60)
    print("DEMO 2: Transaction Abort (Rollback)")
    print("-" * 60)
    print("Demonstrating rollback when errors occur")
    print()

    try:
        producer.begin_transaction()
        print("[TXN] Transaction started")

        # Send some messages
        for seq in range(10, 13):
            event: Dict[str, Any] = {
                "seq": seq,
                "campaign_id": f"campaign_{seq}",
                "spend": round(random.uniform(10.0, 100.0), 2),
                "currency": "USD",
                "transaction": "demo2_aborted"
            }

            key: str = event["campaign_id"]
            value: str = json.dumps(event)

            producer.produce(
                topic=topic_output,
                key=key.encode("utf-8"),
                value=value.encode("utf-8")
            )

            producer.poll(0)
            print(f"[TXN] Produced seq={seq} (buffered)")

            print("Run \n python tutorials/05-transactions/consumer_read_committed_clickhouse.py \n now to see uncommitted messages - seq 10,11,12")
            time.sleep(4)

        # Simulate error during processing
        print("[ERROR] Simulating processing error...")
        raise Exception("Simulated error - validation failed!")

        # This commit will never execute due to exception
        producer.commit_transaction()

    except Exception as e:
        # ABORT TRANSACTION
        # Kafka Concept - TRANSACTION ROLLBACK:
        #   All messages in this transaction are discarded
        #   Consumers will never see these messages
        #   The transaction is as if it never happened
        print(f"[TXN ERROR] Caught error: {e}")
        print("[TXN] Aborting transaction...")
        producer.abort_transaction()
        print("[TXN] Transaction aborted!")
        print("[TXN] Messages seq=10,11,12 were discarded (not visible to consumers)")
        print()

    # ============================================================================
    # DEMO 3: Multi-Topic Atomic Write
    # ============================================================================
    print("-" * 60)
    print("DEMO 3: Multi-Topic Atomic Write")
    print("-" * 60)
    print("Writing to multiple topics atomically")
    print()

    topic_summary: str = "ads_transactions_summary"

    try:
        producer.begin_transaction()
        print("[TXN] Transaction started")

        # Write detailed events to one topic
        total_spend: float = 0.0
        for seq in range(20, 23):
            spend = round(random.uniform(10.0, 100.0), 2)
            total_spend += spend

            event: Dict[str, Any] = {
                "seq": seq,
                "campaign_id": f"campaign_{seq % 3}",
                "spend": spend,
                "currency": "USD",
                "transaction": "demo3"
            }

            producer.produce(
                topic=topic_output,
                key=event["campaign_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8")
            )

            print(f"[TXN] Produced to {topic_output}: seq={seq} spend={spend}")
            time.sleep(4)

        # Write summary to another topic
        summary: Dict[str, Any] = {
            "batch_id": "batch_20_22",
            "message_count": 3,
            "total_spend": round(total_spend, 2),
            "currency": "USD",
            "transaction": "demo3"
        }

        producer.produce(
            topic=topic_summary,
            key="batch_20_22".encode("utf-8"),
            value=json.dumps(summary).encode("utf-8")
        )

        print(f"[TXN] Produced to {topic_summary}: {summary}")

        producer.poll(0)

        # ATOMIC COMMIT across both topics
        print("[TXN] Committing multi-topic transaction...")
        producer.commit_transaction()
        print("[TXN] Transaction committed!")
        print(f"[TXN] Messages in {topic_output} and {topic_summary} are atomically visible")
        print()


    except Exception as e:
        print(f"[TXN ERROR] {e}")
        producer.abort_transaction()

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("=" * 60)
    print("TRANSACTIONS SUMMARY")
    print("=" * 60)
    print("""
What We Demonstrated:
1. Simple transaction (atomic write to single topic)
2. Transaction abort (rollback on error)
3. Multi-topic transaction (atomic write across topics)

Key Benefits:
- ATOMICITY: All messages commit or abort together
- EXACTLY-ONCE: Combined with read_committed consumer
- ERROR RECOVERY: Clean rollback on failures
- MULTI-TOPIC: Coordinate writes across topics

When to Use Transactions:
- Read-process-write pipelines (consume → transform → produce)
- Multi-topic coordination (write to multiple topics atomically)
- Exactly-once processing requirements
- Aggregations (read multiple messages, write summary)

Performance Considerations:
- Transactions have overhead (slower than non-transactional)
- But they prevent duplicates and data loss
- Use for critical data pipelines
- Non-critical streams can use idempotent producer only

Next Steps:
- Run consumer_read_committed.py to consume transactional messages
- Notice how aborted messages are never visible to consumers
- Experiment with crashes during transactions (Ctrl+C before commit)
    """)


if __name__ == "__main__":
    main()

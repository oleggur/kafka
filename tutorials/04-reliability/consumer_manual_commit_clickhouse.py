#!/usr/bin/env python3
"""
Tutorial 04-B: Manual Offset Commit for Reliability
===================================================

This script demonstrates:
- Manual offset management for at-least-once semantics
- Auto-commit pitfalls and data loss scenarios
- Commit strategies (per message, per batch, periodic)
- Error handling and retry logic

Key Concepts:
-------------
1. OFFSET COMMIT: Saving the current read position in Kafka
   - Offsets are stored in special __consumer_offsets topic
   - Committed offset = "I've successfully processed up to here"
   - On restart, consumer resumes from last committed offset

2. AUTO-COMMIT (enable.auto.commit=true):
   - Kafka automatically commits offsets every 5 seconds (default)
   - Simple but RISKY: can lose data or create duplicates
   - Commit happens BEFORE processing (not after!)

3. MANUAL COMMIT (enable.auto.commit=false):
   - YOU control when to commit
   - Commit AFTER successful processing
   - More code but safer (at-least-once guarantee)

4. AT-LEAST-ONCE vs AT-MOST-ONCE vs EXACTLY-ONCE:
   - AT-MOST-ONCE: Commit before processing (auto-commit)
     Risk: Process crashes → message lost (not reprocessed)
   - AT-LEAST-ONCE: Commit after processing (manual commit)
     Risk: Process crashes → message reprocessed (possible duplicate)
   - EXACTLY-ONCE: Transactional processing (Tutorial 05)

Auto-Commit Pitfall Example:
----------------------------
1. Consumer polls and gets messages 1-100
2. Auto-commit commits offset 100 (every 5 seconds)
3. Consumer starts processing message 1
4. Consumer crashes at message 50
5. On restart, consumer resumes at offset 100 (skipped 51-100!)
6. Messages 51-100 are LOST (never processed)

Manual Commit Solution:
-----------------------
1. Consumer polls and gets message 1
2. Consumer processes message 1
3. Consumer writes to ClickHouse
4. Consumer commits offset 1 (AFTER processing)
5. Consumer crashes
6. On restart, consumer resumes at offset 2
7. No data loss!
"""

import json
import time
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message, TopicPartition
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def main() -> None:
    """
    Demonstrate manual offset management for reliable processing.
    """

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "reliability_manual_commit",
        "auto.offset.reset": "earliest",

        # DISABLE AUTO-COMMIT
        # Kafka Concept - MANUAL COMMIT:
        #   We take full control of when offsets are committed
        #   This allows us to commit AFTER successful processing
        #   Ensures at-least-once semantics (no data loss)
        "enable.auto.commit": False,

        # SESSION.TIMEOUT: How long before consumer is considered dead
        # If we take too long to process, Kafka may think we crashed
        # and trigger a rebalance (careful with long processing times!)
        "session.timeout.ms": 30000,  # 30 seconds

        # MAX.POLL.INTERVAL.MS: Max time between poll() calls
        # If we don't call poll() within this time, Kafka kicks us out
        # Make this longer if you have slow processing
        "max.poll.interval.ms": 300000,  # 5 minutes
    }

    consumer: Consumer = Consumer(consumer_config)
    topic: str = "ads_reliability_idem"  # Use idempotent topic from previous demo
    consumer.subscribe([topic])

    print("=" * 60)
    print("MANUAL OFFSET COMMIT TUTORIAL")
    print("=" * 60)
    print(f"Topic: {topic}")
    print(f"Group: {consumer_config['group.id']}")
    print(f"Auto-commit: DISABLED (manual control)")
    print("-" * 60)
    print()

    # ============================================================================
    # CLICKHOUSE SETUP
    # ============================================================================

    clickhouse_client: Client = get_client(
        host="localhost",
        port=8123,
        username="default",
        password="secret",
        database="demo"
    )

    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS ads_reliability (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Float64,
            currency String,
            idempotent Boolean,
            processing_time_ms UInt32
        ) ENGINE = MergeTree()
        ORDER BY (campaign_id, seq)
    """)

    print("[CLICKHOUSE] Table 'ads_reliability' ready")
    print()

    # ============================================================================
    # CONSUME WITH MANUAL COMMIT
    # ============================================================================

    message_count: int = 0
    commit_count: int = 0

    # COMMIT STRATEGY: We'll demonstrate different strategies
    COMMIT_STRATEGY = "per_message"  # Options: per_message, per_batch, periodic

    batch_size: int = 10
    messages_since_commit: int = 0
    last_commit_time: float = time.time()
    commit_interval_seconds: float = 5.0

    try:
        print(f"[STRATEGY] Commit strategy: {COMMIT_STRATEGY}")
        print()

        while True:
            # POLL: Get next message
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                # No message - maybe check if we should commit (for periodic strategy)
                if COMMIT_STRATEGY == "periodic":
                    if time.time() - last_commit_time >= commit_interval_seconds and messages_since_commit > 0:
                        consumer.commit(asynchronous=False)
                        commit_count += 1
                        print(f"[COMMIT] Periodic commit ({messages_since_commit} messages)")
                        messages_since_commit = 0
                        last_commit_time = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # ========================================================================
            # PROCESS MESSAGE
            # ========================================================================

            start_time = time.time()

            partition: int = msg.partition()
            offset: int = msg.offset()

            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue

            data: Dict[str, Any] = json.loads(value_bytes.decode("utf-8"))

            seq: int = int(data["seq"])
            campaign_id: str = str(data["campaign_id"])
            spend: float = float(data["spend"])
            currency: str = str(data["currency"])
            idempotent: bool = bool(data.get("idempotent", False))

            message_count += 1

            print(
                f"[RECV #{message_count}] partition={partition} offset={offset} | "
                f"seq={seq} campaign={campaign_id}"
            )

            # Simulate processing time (in real app, this might be database write, API call, etc.)
            time.sleep(0.01)

            processing_time_ms: int = int((time.time() - start_time) * 1000)

            # ========================================================================
            # WRITE TO CLICKHOUSE
            # ========================================================================
            # This is the CRITICAL OPERATION
            # We must ensure this succeeds before committing the offset
            # Otherwise we might skip messages on restart

            try:
                clickhouse_client.insert(
                    table="ads_reliability",
                    data=[(
                        seq,
                        campaign_id,
                        spend,
                        currency,
                        idempotent,
                        processing_time_ms
                    )],
                    column_names=[
                        "seq",
                        "campaign_id",
                        "spend",
                        "currency",
                        "idempotent",
                        "processing_time_ms"
                    ]
                )

                print(f"[CLICKHOUSE] Inserted seq={seq}")

            except Exception as e:
                # ClickHouse write failed!
                # DON'T commit offset - we'll retry this message after restart
                print(f"[ERROR] ClickHouse write failed: {e}")
                print(f"[ERROR] NOT committing offset (will retry on restart)")
                # In production, you might:
                # - Retry with backoff
                # - Send to dead letter queue
                # - Alert monitoring system
                continue  # Skip commit, move to next message

            # ========================================================================
            # COMMIT OFFSET (AFTER SUCCESSFUL PROCESSING)
            # ========================================================================
            # Kafka Concept - AT-LEAST-ONCE:
            #   We commit AFTER writing to ClickHouse
            #   If we crash before commit, we'll reprocess this message
            #   ClickHouse might see duplicates (handle with deduplication)

            messages_since_commit += 1

            if COMMIT_STRATEGY == "per_message":
                # STRATEGY 1: Commit after every message
                # Pros: Minimal reprocessing on crash (at most 1 message)
                # Cons: High commit overhead (slow for high throughput)

                consumer.commit(asynchronous=False)
                commit_count += 1
                print(f"[COMMIT] Committed offset {offset + 1} (per-message)")

            elif COMMIT_STRATEGY == "per_batch" and messages_since_commit >= batch_size:
                # STRATEGY 2: Commit after every N messages
                # Pros: Lower commit overhead (faster)
                # Cons: May reprocess up to N messages on crash

                consumer.commit(asynchronous=False)
                commit_count += 1
                print(f"[COMMIT] Committed batch of {messages_since_commit} messages")
                messages_since_commit = 0

            elif COMMIT_STRATEGY == "periodic":
                # STRATEGY 3: Commit every T seconds
                # Handled in the msg is None block above
                pass

            print()

    except KeyboardInterrupt:
        print("-" * 60)
        print(f"[STOP] Shutting down...")
        print(f"  Messages processed: {message_count}")
        print(f"  Commits made: {commit_count}")

        # Final commit before shutdown
        if messages_since_commit > 0:
            print(f"[COMMIT] Final commit of {messages_since_commit} messages")
            consumer.commit(asynchronous=False)

    finally:
        # Close consumer (triggers final commit if auto-commit was enabled)
        consumer.close()
        print("[CONSUMER] Closed")

        # ========================================================================
        # SHOW RESULTS
        # ========================================================================
        print("\n" + "=" * 60)
        print("CLICKHOUSE STATISTICS")
        print("=" * 60)

        result = clickhouse_client.query("SELECT COUNT(*) FROM ads_reliability")
        total_rows = result.result_rows[0][0]
        print(f"Total rows: {total_rows}")

        result = clickhouse_client.query("""
            SELECT AVG(processing_time_ms) as avg_ms
            FROM ads_reliability
        """)
        avg_processing_ms = result.result_rows[0][0]
        print(f"Average processing time: {avg_processing_ms:.2f} ms")

        # ========================================================================
        # COMMIT STRATEGIES SUMMARY
        # ========================================================================
        print("\n" + "=" * 60)
        print("COMMIT STRATEGIES COMPARISON")
        print("=" * 60)
        print("""
1. PER-MESSAGE COMMIT:
   - Commit after each message
   - Lowest reprocessing risk (max 1 message duplicated)
   - Highest commit overhead (can be slow)
   - Use for: Low throughput, critical data

2. PER-BATCH COMMIT:
   - Commit after N messages
   - Moderate reprocessing risk (max N messages duplicated)
   - Lower commit overhead (faster)
   - Use for: High throughput, can tolerate some duplicates

3. PERIODIC COMMIT:
   - Commit every T seconds
   - Variable reprocessing risk (depends on throughput)
   - Lowest commit overhead
   - Use for: Very high throughput, idempotent processing

RECOMMENDATION: Use per-batch with small batch size (10-100)
  - Good balance of performance and reliability
  - Combined with idempotent producer (no duplicates on send)
  - Handle duplicates in consumer (deduplication in ClickHouse)
        """)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Tutorial 04-B: Auto-Commit vs Manual Commit Comparison
=======================================================

This script demonstrates:
- Auto-commit mode (at-most-once) - risk of data LOSS
- Manual commit mode (at-least-once) - risk of DUPLICATES but no loss
- Commit strategies (per message, per batch, periodic)
- Error handling and retry logic

Usage:
------
python consumer_manual_commit_clickhouse.py auto      # Auto-commit (data loss risk)
python consumer_manual_commit_clickhouse.py manual    # Manual commit (default, safe)

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
import sys
import time
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message, TopicPartition
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def main() -> None:
    """
    Demonstrate auto-commit vs manual commit offset management.
    """

    # ============================================================================
    # PARSE COMMAND LINE ARGUMENTS
    # ============================================================================

    commit_mode = "manual"  # default
    if len(sys.argv) > 1:
        commit_mode = sys.argv[1].lower()
        if commit_mode not in ["auto", "manual"]:
            print("Usage: python consumer_manual_commit_clickhouse.py [auto|manual]")
            print("  auto   - Enable auto-commit (demonstrates data loss)")
            print("  manual - Disable auto-commit (demonstrates duplicates but no loss)")
            sys.exit(1)

    use_auto_commit = (commit_mode == "auto")

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"reliability_{commit_mode}_commit",
        "auto.offset.reset": "earliest",

        # AUTO-COMMIT vs MANUAL COMMIT
        # Kafka Concept:
        #   AUTO-COMMIT (enable.auto.commit=True):
        #     - Kafka commits offsets every N seconds (default 5s)
        #     - Commits based on what was FETCHED, not PROCESSED
        #     - Risk: Data loss if crash before processing
        #   MANUAL COMMIT (enable.auto.commit=False):
        #     - You control when to commit
        #     - Commit AFTER successful processing
        #     - Risk: Duplicates if crash before commit, but NO DATA LOSS
        "enable.auto.commit": use_auto_commit,

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
    if use_auto_commit:
        print("AUTO-COMMIT MODE (AT-MOST-ONCE - DATA LOSS RISK!)")
    else:
        print("MANUAL COMMIT MODE (AT-LEAST-ONCE - NO DATA LOSS)")
    print("=" * 60)
    print(f"Topic: {topic}")
    print(f"Group: {consumer_config['group.id']}")
    print(f"Auto-commit: {'ENABLED' if use_auto_commit else 'DISABLED'}")
    if use_auto_commit:
        print(f"Auto-commit interval: {consumer_config.get('auto.commit.interval.ms', 5000)}ms")
        print("⚠️  WARNING: Auto-commit can cause DATA LOSS on crash!")
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
    # CONSUME WITH AUTO OR MANUAL COMMIT
    # ============================================================================

    message_count: int = 0
    commit_count: int = 0
    messages_since_commit: int = 0

    # Track auto-commit timing
    auto_commit_warning_shown: bool = False
    consumer_start_time: float = time.time()

    try:
        if use_auto_commit:
            print("[INFO] Press Ctrl+C anytime to simulate crash")
        print()

        while True:
            # POLL: Get next message
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
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

            # Simulate SLOW processing BEFORE database write
            # This is KEY to demonstrating the difference:
            # - AUTO-COMMIT: May commit while we're sleeping (before insert) → DATA LOSS on crash
            # - MANUAL COMMIT: Won't commit until after insert → NO DATA LOSS on crash

            # Show warning if auto-commit interval has passed
            if use_auto_commit and not auto_commit_warning_shown:
                elapsed = time.time() - consumer_start_time
                if elapsed >= 5.0:  # Auto-commit interval
                    print("⚠️  [AUTO-COMMIT] 5 seconds elapsed - Kafka has committed offsets in background!")
                    print("⚠️  [AUTO-COMMIT] If you press Ctrl+C now, unprocessed messages will be LOST!")
                    auto_commit_warning_shown = True

            print(f"[PROCESSING] Processing message {message_count}...")
            time.sleep(2)  # 2 seconds of "processing" before writing to database

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
                        int((time.time() - start_time) * 1000)
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

            # Only do manual commits if auto-commit is disabled
            if not use_auto_commit:
                # Manual commit after each message
                consumer.commit(asynchronous=False)
                commit_count += 1
                print(f"[COMMIT] Committed offset {offset + 1}")
            else:
                # Auto-commit mode: Kafka commits in background automatically
                print(f"[AUTO-COMMIT] Offset {offset} will be committed automatically in background")

            print()

    except KeyboardInterrupt:
        print("-" * 60)
        print(f"[STOP] Crash simulated!")
        print(f"  Messages processed: {message_count}")
        if use_auto_commit:
            print(f"  Commits: AUTO (Kafka committed in background every 5s)")
            print()
            print("[WARNING] Auto-commit may have already committed offsets for unfetched messages!")
            print("[WARNING] On restart, you may see MISSING messages (data loss)")
        else:
            print(f"  Manual commits made: {commit_count}")
            print(f"  Uncommitted messages: {messages_since_commit}")
            print()
            print("[INFO] On restart, uncommitted messages will be reprocessed (no data loss)")

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

        # Check for duplicates
        print("\nChecking for duplicates:")
        result = clickhouse_client.query("""
            SELECT seq, COUNT(*) as count
            FROM ads_reliability
            GROUP BY seq
            ORDER BY seq
        """)
        for row in result.result_rows:
            seq, count = row
            if count > 1:
                print(f"  seq={seq}: {count} times (DUPLICATE!)")
            else:
                print(f"  seq={seq}: {count} time")

        print("\nTo check data in ClickHouse:")
        print("  docker exec -it clickhouse clickhouse-client --password secret")
        print("  SELECT seq, COUNT(*) as count FROM demo.ads_reliability GROUP BY seq ORDER BY seq;")
        print("  SELECT * FROM demo.ads_reliability ORDER BY seq;")



if __name__ == "__main__":
    main()

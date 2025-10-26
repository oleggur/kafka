#!/usr/bin/env python3
"""
Tutorial 05: Read Committed Consumer (Exactly-Once Semantics)
=============================================================

This script demonstrates:
- Reading only committed transactions
- Isolation levels (read_uncommitted vs read_committed)
- How consumers interact with transactional producers
- Exactly-once end-to-end processing

Key Concepts:
-------------
1. ISOLATION LEVEL: Controls which messages are visible to consumer
   - read_uncommitted (default): See all messages immediately
   - read_committed: Only see committed transactions

2. READ_COMMITTED:
   - Aborted transactions are never visible
   - Messages appear atomically (all or nothing)
   - Required for exactly-once semantics (EOS)

3. EXACTLY-ONCE END-TO-END:
   Producer Side:
   - enable.idempotence=true (prevents duplicates on send)
   - Transactional producer (atomic commits)

   Consumer Side:
   - isolation.level=read_committed (only see committed data)
   - Manual offset management (commit after processing)

   Result: Each message processed exactly once

4. TRANSACTION MARKERS:
   - Kafka writes control messages (COMMIT/ABORT) after transactions
   - Consumers with read_committed filter based on these markers
   - Aborted messages are skipped automatically

How It Works:
-------------
Timeline with Transactional Producer:

T1: Producer begins transaction
T2: Producer writes message A (buffered)
T3: Producer writes message B (buffered)
T4: Producer commits transaction

read_uncommitted consumer: Sees A at T2, B at T3
read_committed consumer: Sees A and B together at T4 (atomic)

If producer aborted instead:
read_uncommitted: Sees A and B (includes aborted data!)
read_committed: Never sees A or B (correctly filtered)
"""

import json
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def demo_read_uncommitted() -> None:
    """
    Demonstrate read_uncommitted (default) - sees all messages including aborted.
    """
    print("=" * 60)
    print("DEMO 1: read_uncommitted (Default Behavior)")
    print("=" * 60)
    print("Consumer will see ALL messages, including aborted transactions")
    print()

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "txn_read_uncommitted",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,

        # ISOLATION.LEVEL: read_uncommitted (default)
        # Consumer sees messages immediately, before transaction commits
        # This can include messages from aborted transactions!
        "isolation.level": "read_uncommitted",
    }

    consumer: Consumer = Consumer(consumer_config)
    topic: str = "ads_transactions_output"
    consumer.subscribe([topic])

    print(f"[CONFIG] Isolation level: read_uncommitted")
    print(f"[CONFIG] Topic: {topic}")
    print("-" * 60)
    print()

    message_count: int = 0
    aborted_count: int = 0

    try:
        # Poll for 5 seconds
        start_time = import_time = __import__('time').time()
        timeout: float = 5.0

        while __import__('time').time() - start_time < timeout:
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue

            data: Dict[str, Any] = json.loads(value_bytes.decode("utf-8"))
            transaction_id = data.get("transaction", "unknown")

            message_count += 1

            # Check if this is from an aborted transaction (demo2_aborted)
            if "aborted" in transaction_id:
                aborted_count += 1
                print(f"[RECV #{message_count}] ⚠️  Aborted transaction: seq={data['seq']} txn={transaction_id}")
            else:
                print(f"[RECV #{message_count}] seq={data['seq']} txn={transaction_id}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

        print()
        print("-" * 60)
        print(f"[RESULT] Total messages: {message_count}")
        print(f"[RESULT] Aborted messages: {aborted_count}")
        if aborted_count > 0:
            print(f"[WARNING] With read_uncommitted, you saw {aborted_count} messages")
            print("           that were supposed to be rolled back!")
        print()


def demo_read_committed_clickhouse() -> None:
    """
    Demonstrate read_committed - only sees committed transactions.
    """
    print("=" * 60)
    print("DEMO 2: read_committed (Exactly-Once Semantics)")
    print("=" * 60)
    print("Consumer will ONLY see committed transactions")
    print("Aborted transactions are automatically filtered out")
    print()

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "txn_read_committed_clickhouse",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,

        # ISOLATION.LEVEL: read_committed
        # Kafka Concept - READ COMMITTED:
        #   - Consumer only sees messages from committed transactions
        #   - Aborted transactions are automatically skipped
        #   - Messages from a transaction appear atomically (all at once)
        #   - Required for exactly-once end-to-end processing
        "isolation.level": "read_committed",
    }

    consumer: Consumer = Consumer(consumer_config)
    topic: str = "ads_transactions_output"
    consumer.subscribe([topic])

    print(f"[CONFIG] Isolation level: read_committed")
    print(f"[CONFIG] Topic: {topic}")
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
        CREATE TABLE IF NOT EXISTS ads_transactions (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Float64,
            currency String,
            transaction String
        ) ENGINE = MergeTree()
        ORDER BY (transaction, seq)
    """)

    print("[CLICKHOUSE] Table 'ads_transactions' ready")
    print()

    # ============================================================================
    # CONSUME WITH READ_COMMITTED
    # ============================================================================

    message_count: int = 0
    transactions_seen: set = set()

    try:
        # Poll for 5 seconds
        start_time = __import__('time').time()
        timeout: float = 5.0

        while __import__('time').time() - start_time < timeout:
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue

            data: Dict[str, Any] = json.loads(value_bytes.decode("utf-8"))

            seq: int = int(data["seq"])
            campaign_id: str = str(data["campaign_id"])
            spend: float = float(data["spend"])
            currency: str = str(data["currency"])
            transaction_id: str = data.get("transaction", "unknown")

            transactions_seen.add(transaction_id)
            message_count += 1

            print(
                f"[RECV #{message_count}] seq={seq} txn={transaction_id} | "
                f"campaign={campaign_id} spend={spend}"
            )

            # Write to ClickHouse
            clickhouse_client.insert(
                table="ads_transactions",
                data=[(seq, campaign_id, spend, currency, transaction_id)],
                column_names=["seq", "campaign_id", "spend", "currency", "transaction"]
            )

            # Commit offset after successful processing
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

        print()
        print("-" * 60)
        print(f"[RESULT] Total messages: {message_count}")
        print(f"[RESULT] Transactions seen: {sorted(transactions_seen)}")
        print()

        if "demo2_aborted" not in transactions_seen:
            print("[SUCCESS] ✓ Aborted transaction (demo2_aborted) was correctly filtered!")
            print("           You did NOT see messages that were rolled back")
        else:
            print("[ERROR] ✗ Saw messages from aborted transaction (unexpected!)")

        print()

        # Show ClickHouse data
        result = clickhouse_client.query("SELECT COUNT(*) FROM ads_transactions")
        total_rows = result.result_rows[0][0]
        print(f"[CLICKHOUSE] Total rows: {total_rows}")

        result = clickhouse_client.query("""
            SELECT transaction, COUNT(*) as count
            FROM ads_transactions
            GROUP BY transaction
            ORDER BY transaction
        """)
        print("\nMessages per transaction:")
        for row in result.result_rows:
            print(f"  {row[0]}: {row[1]} messages")


def main() -> None:
    """
    Run isolation level demos.
    """

    print("\n" + "=" * 60)
    print("KAFKA ISOLATION LEVELS TUTORIAL")
    print("=" * 60)
    print()
    print("This tutorial demonstrates the difference between:")
    print("  1. read_uncommitted (default) - sees all messages")
    print("  2. read_committed - only sees committed transactions")
    print()
    print("We'll consume from topics written by the transactional producer")
    print()

    # Demo 1: read_uncommitted (sees aborted transactions)
    demo_read_uncommitted()

    import time
    time.sleep(2)

    # Demo 2: read_committed with ClickHouse (filters aborted transactions)
    demo_read_committed_clickhouse()

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("=" * 60)
    print("ISOLATION LEVELS SUMMARY")
    print("=" * 60)
    print("""
read_uncommitted (default):
- Sees all messages immediately (before transaction commits)
- Can see messages from aborted transactions (BAD!)
- Faster (no waiting for commit markers)
- Use for: Non-critical streams, when duplicates are acceptable

read_committed:
- Only sees messages from committed transactions
- Aborted transactions are automatically filtered
- Messages from a transaction appear atomically
- Required for exactly-once semantics
- Use for: Critical data, when duplicates are unacceptable

Exactly-Once End-to-End Recipe:
1. Producer:
   - enable.idempotence=true
   - Use transactional producer
   - Commit transactions after writes

2. Consumer:
   - isolation.level=read_committed
   - enable.auto.commit=false (manual commit)
   - Commit offset AFTER processing (at-least-once)

3. Processing:
   - Make processing idempotent (handle duplicates gracefully)
   - Or use ClickHouse deduplication (ReplacingMergeTree, etc.)

Result: Each message processed exactly once, even with failures!
    """)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Tutorial 03: Parallel Consumers with Partitions
===============================================

This script demonstrates:
- How partitions enable parallel consumption
- Consumer group coordination and partition assignment
- How to run multiple consumers in the same group
- Partition rebalancing when consumers join/leave

Key Concepts:
-------------
1. CONSUMER GROUP: Consumers with the same group.id work together
   - Kafka assigns partitions to consumers in the group
   - Each partition is consumed by exactly ONE consumer in the group
   - If consumers = partitions: 1:1 assignment
   - If consumers < partitions: some consumers get multiple partitions
   - If consumers > partitions: some consumers are idle

2. PARTITION ASSIGNMENT: How Kafka distributes partitions to consumers
   - Automatic assignment via subscribe()
   - Kafka ensures balanced distribution
   - When a consumer joins/leaves, partitions are REBALANCED

3. REBALANCING: Redistribution of partitions when group membership changes
   - Triggered by: new consumer, consumer crash, consumer timeout
   - During rebalance: consumption pauses briefly
   - After rebalance: partitions are reassigned

4. OFFSET MANAGEMENT: Each partition has its own offset
   - Offset tracks position in the partition
   - Committed offsets are stored per consumer group + partition
   - Different consumer groups have independent offsets

Running Multiple Consumers:
---------------------------
To see parallel consumption in action:
1. Run this script in one terminal (Consumer 1)
2. Run it again in another terminal (Consumer 2)
3. Optionally run a third time (Consumer 3)
4. Watch how partitions are distributed across consumers
"""

import json
import sys
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def main() -> None:
    """
    Parallel consumer that demonstrates partition assignment.
    """

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================

    # Optional: Pass a consumer instance ID as command line argument
    # This helps identify which consumer is which when running multiple instances
    consumer_instance: str = sys.argv[1] if len(sys.argv) > 1 else "default"

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",

        # CONSUMER GROUP: All consumers with same group.id coordinate
        # Try running multiple instances of this script - they'll share partitions!
        "group.id": "partitioned_parallel_group",

        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,

        # SESSION.TIMEOUT.MS: Max time without heartbeat before considered dead
        # If a consumer doesn't send heartbeat within this time, it's kicked out
        # and partitions are rebalanced to remaining consumers
        "session.timeout.ms": 10000,  # 10 seconds

        # HEARTBEAT.INTERVAL.MS: How often to send heartbeats
        # Must be lower than session.timeout.ms
        "heartbeat.interval.ms": 3000,  # 3 seconds
    }

    consumer: Consumer = Consumer(consumer_config)
    topic: str = "ads_partitioned"
    consumer.subscribe([topic])

    print("=" * 60)
    print(f"CONSUMER INSTANCE: {consumer_instance}")
    print("=" * 60)
    print(f"Topic: {topic}")
    print(f"Group: {consumer_config['group.id']}")
    print("-" * 60)
    print("This consumer will coordinate with other consumers in the same group")
    print("to parallelize consumption across partitions.")
    print()
    print("To see parallel consumption:")
    print("  1. Run this script in multiple terminals")
    print("  2. Each instance will be assigned different partitions")
    print("  3. Kill one instance and watch partitions rebalance")
    print("=" * 60)
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

    # Create table for partitioned data
    # We add partition_id column to track which partition messages came from
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS ads_partitioned (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Float64,
            currency String,
            strategy String,
            partition_id UInt32,              -- Track which Kafka partition
            consumer_instance String           -- Track which consumer instance
        ) ENGINE = MergeTree()
        ORDER BY (partition_id, seq)
    """)

    print("[CLICKHOUSE] Table 'ads_partitioned' ready")
    print()

    # ============================================================================
    # CONSUME WITH PARTITION TRACKING
    # ============================================================================

    message_count: int = 0
    partitions_seen: set = set()

    try:
        while True:
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                # No message - show current partition assignment
                # Kafka Concept - PARTITION ASSIGNMENT:
                #   assignment() returns the partitions currently assigned to this consumer
                assignment = consumer.assignment()
                if assignment and len(partitions_seen) == 0:
                    # First assignment received
                    partition_list = [f"partition-{tp.partition}" for tp in assignment]
                    print(f"[ASSIGNMENT] Consumer {consumer_instance} assigned: {', '.join(partition_list)}")
                    print()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # ========================================================================
            # PROCESS MESSAGE
            # ========================================================================

            partition: int = msg.partition()
            offset: int = msg.offset()

            # Track which partitions this consumer is reading from
            if partition not in partitions_seen:
                partitions_seen.add(partition)
                print(f"[NEW PARTITION] Consumer {consumer_instance} now reading partition {partition}")
                print()

            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue

            data: Dict[str, Any] = json.loads(value_bytes.decode("utf-8"))

            seq: int = int(data["seq"])
            campaign_id: str = str(data["campaign_id"])
            spend: float = float(data["spend"])
            currency: str = str(data["currency"])
            strategy: str = str(data.get("strategy", "unknown"))

            message_count += 1

            print(
                f"[RECV #{message_count}] Consumer={consumer_instance} | "
                f"Partition={partition} Offset={offset} | "
                f"Campaign={campaign_id} Strategy={strategy}"
            )

            # ========================================================================
            # INSERT INTO CLICKHOUSE
            # ========================================================================
            clickhouse_client.insert(
                table="ads_partitioned",
                data=[(
                    seq,
                    campaign_id,
                    spend,
                    currency,
                    strategy,
                    partition,
                    consumer_instance
                )],
                column_names=[
                    "seq",
                    "campaign_id",
                    "spend",
                    "currency",
                    "strategy",
                    "partition_id",
                    "consumer_instance"
                ]
            )

    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print(f"[STOP] Consumer {consumer_instance} shutting down...")
        print(f"  Messages processed: {message_count}")
        print(f"  Partitions read: {sorted(partitions_seen)}")

    finally:
        # Kafka Concept - GRACEFUL SHUTDOWN:
        # When we call close(), Kafka triggers a rebalance
        # Our partitions will be reassigned to remaining consumers in the group
        print(f"[REBALANCE] Consumer {consumer_instance} leaving group...")
        print("  (Partitions will be reassigned to remaining consumers)")

        consumer.close()
        print(f"[CONSUMER] {consumer_instance} closed")

        # Show ClickHouse statistics
        print("\n" + "=" * 60)
        print("CLICKHOUSE STATISTICS")
        print("=" * 60)

        # Total rows
        result = clickhouse_client.query("SELECT COUNT(*) FROM ads_partitioned")
        total_rows = result.result_rows[0][0]
        print(f"Total rows: {total_rows}")

        # Messages per partition
        result = clickhouse_client.query("""
            SELECT partition_id, COUNT(*) as count
            FROM ads_partitioned
            GROUP BY partition_id
            ORDER BY partition_id
        """)
        print("\nMessages per partition:")
        for row in result.result_rows:
            print(f"  Partition {row[0]}: {row[1]} messages")

        # Messages per consumer instance
        result = clickhouse_client.query("""
            SELECT consumer_instance, COUNT(*) as count
            FROM ads_partitioned
            GROUP BY consumer_instance
            ORDER BY consumer_instance
        """)
        print("\nMessages per consumer instance:")
        for row in result.result_rows:
            print(f"  {row[0]}: {row[1]} messages")

        # Messages per strategy
        result = clickhouse_client.query("""
            SELECT strategy, COUNT(*) as count
            FROM ads_partitioned
            GROUP BY strategy
            ORDER BY strategy
        """)
        print("\nMessages per partitioning strategy:")
        for row in result.result_rows:
            print(f"  {row[0]}: {row[1]} messages")


if __name__ == "__main__":
    main()

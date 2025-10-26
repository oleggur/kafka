#!/usr/bin/env python3
"""
Tutorial 03: Kafka Partitioning
================================

This script demonstrates:
- How Kafka partitions topics for parallelism
- How message keys control partition assignment
- How to manually specify partitions
- Partition strategies and their trade-offs

Key Concepts:
-------------
1. PARTITION: A topic is split into partitions (like shards in databases)
   - Each partition is an ordered, immutable log of messages
   - Partitions enable parallelism (multiple consumers can read in parallel)
   - Messages within a partition are strictly ordered
   - Messages across partitions have no ordering guarantee

2. PARTITION KEY: A value used to determine which partition gets the message
   - Same key -> same partition (ensures ordering for related messages)
   - Kafka hashes the key: partition = hash(key) % num_partitions
   - If no key: round-robin or random partition assignment

3. PARTITION COUNT: Number of partitions in a topic
   - More partitions = more parallelism (more consumers)
   - But more partitions = more overhead (file descriptors, memory)
   - Typical range: 3-30 partitions per topic

4. PARTITION ASSIGNMENT: How messages are distributed
   - Key-based: hash(key) % num_partitions (default)
   - Round-robin: no key, distribute evenly
   - Manual: explicitly specify partition number

Why Partitioning Matters:
-------------------------
- SCALABILITY: More partitions = more parallel consumers
- ORDERING: Messages with same key stay in order
- LOCALITY: Related messages stay together (cache-friendly)
- FAULT TOLERANCE: Replicas are distributed across partitions
"""

import json
import random
import time
from typing import Dict, Any, Optional

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """
    Delivery callback that shows which partition the message went to.
    """
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()} key={msg.key().decode('utf-8') if msg.key() else None}"
        )


def create_topic_with_partitions(topic_name: str, num_partitions: int) -> None:
    """
    Create a topic with a specific number of partitions.

    Args:
        topic_name: Name of the topic to create
        num_partitions: Number of partitions for this topic
    """
    admin_config: Dict[str, str] = {
        "bootstrap.servers": "localhost:9092"
    }

    admin_client: AdminClient = AdminClient(admin_config)

    # Check if topic already exists
    metadata = admin_client.list_topics(timeout=5)
    if topic_name in metadata.topics:
        print(f"[INFO] Topic '{topic_name}' already exists")
        print(f"  Partitions: {len(metadata.topics[topic_name].partitions)}")
        return

    # Create topic with specified partitions
    # Kafka Concept - REPLICATION FACTOR:
    #   How many copies of each partition to maintain
    #   1 = no replication (only for dev/testing)
    #   3 = production standard (tolerates 2 broker failures)
    topic: NewTopic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=1  # 1 for single-broker setup
    )

    futures = admin_client.create_topics([topic])

    # Wait for topic creation to complete
    for topic_name, future in futures.items():
        try:
            future.result()  # Block until complete
            print(f"[CREATED] Topic '{topic_name}' with {num_partitions} partitions")
        except Exception as e:
            print(f"[ERROR] Failed to create topic: {e}")


def main() -> None:
    """
    Demonstrate different partitioning strategies.
    """

    # ============================================================================
    # TOPIC SETUP
    # ============================================================================
    # Create a topic with 3 partitions to demonstrate partitioning

    topic: str = "ads_partitioned"
    num_partitions: int = 3

    create_topic_with_partitions(topic, num_partitions)

    # ============================================================================
    # PRODUCER CONFIGURATION
    # ============================================================================

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,
    }

    producer: Producer = Producer(producer_config)

    print("\n" + "=" * 60)
    print("PARTITIONING STRATEGIES DEMO")
    print("=" * 60)

    # ============================================================================
    # STRATEGY 1: KEY-BASED PARTITIONING (Default)
    # ============================================================================
    # Messages with the same key always go to the same partition
    # This ensures ORDERING for related messages

    print("\n" + "-" * 60)
    print("STRATEGY 1: Key-Based Partitioning")
    print("-" * 60)
    print("Messages with the same key go to the same partition")
    print("This ensures ordering within a key (e.g., all events for campaign_A)")
    print()

    campaigns = ["campaign_A", "campaign_B", "campaign_C"]

    for seq in range(9):  # 9 messages, 3 for each campaign
        campaign_id = campaigns[seq % 3]

        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": campaign_id,
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "strategy": "key-based"
        }

        # KEY-BASED PARTITIONING:
        # Kafka hashes the key and assigns partition: hash(key) % num_partitions
        # Same key -> same hash -> same partition (consistent hashing)
        key: str = campaign_id
        value: str = json.dumps(event)

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            on_delivery=delivery_report
        )

        producer.poll(0)
        time.sleep(0.1)

    producer.flush()

    print("\n[OBSERVATION] Notice how:")
    print("  - All 'campaign_A' messages went to the same partition")
    print("  - All 'campaign_B' messages went to the same partition")
    print("  - All 'campaign_C' messages went to the same partition")
    print("  - This ensures ordering within each campaign")

    # ============================================================================
    # STRATEGY 2: NULL KEY (Round-Robin)
    # ============================================================================
    # Without a key, Kafka distributes messages evenly across partitions

    print("\n" + "-" * 60)
    print("STRATEGY 2: Null Key (Round-Robin)")
    print("-" * 60)
    print("Messages without a key are distributed evenly across partitions")
    print("Use when you don't need ordering guarantees")
    print()

    for seq in range(10, 16):  # 6 messages with no key
        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": f"campaign_{seq}",
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "strategy": "null-key"
        }

        value: str = json.dumps(event)

        # NO KEY: Messages are distributed using round-robin or random strategy
        producer.produce(
            topic=topic,
            key=None,  # No key!
            value=value.encode("utf-8"),
            on_delivery=delivery_report
        )

        producer.poll(0)
        time.sleep(0.1)

    producer.flush()

    print("\n[OBSERVATION] Notice how:")
    print("  - Messages are distributed across different partitions")
    print("  - No ordering guarantee (messages may be consumed out of order)")
    print("  - Good for high throughput when order doesn't matter")

    # ============================================================================
    # STRATEGY 3: MANUAL PARTITION ASSIGNMENT
    # ============================================================================
    # Explicitly specify which partition to use

    print("\n" + "-" * 60)
    print("STRATEGY 3: Manual Partition Assignment")
    print("-" * 60)
    print("Explicitly control which partition gets each message")
    print("Use when you need custom partitioning logic")
    print()

    for seq in range(20, 23):  # 3 messages, one per partition
        partition_id = seq - 20  # 0, 1, 2

        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": f"campaign_{seq}",
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "strategy": "manual",
            "target_partition": partition_id
        }

        value: str = json.dumps(event)

        # MANUAL PARTITION:
        # Directly specify the partition number
        producer.produce(
            topic=topic,
            partition=partition_id,  # Explicitly set partition
            key=None,
            value=value.encode("utf-8"),
            on_delivery=delivery_report
        )

        producer.poll(0)
        time.sleep(0.1)

    producer.flush()

    print("\n[OBSERVATION] Notice how:")
    print("  - Each message went to the exact partition we specified")
    print("  - Useful for custom partitioning logic (e.g., by region, priority)")

    # ============================================================================
    # PARTITIONING BEST PRACTICES
    # ============================================================================
    print("\n" + "=" * 60)
    print("PARTITIONING BEST PRACTICES")
    print("=" * 60)
    print("""
1. Use KEY-BASED partitioning when:
   - You need ordering within a logical group (e.g., user_id, order_id)
   - Related events should be processed together
   - You want cache locality (same consumer processes related data)

2. Use NULL KEY (round-robin) when:
   - Order doesn't matter
   - You want maximum throughput
   - Messages are independent

3. Use MANUAL partitioning when:
   - You have custom business logic (e.g., region-based routing)
   - You need strict control over partition assignment
   - You're implementing a custom partitioner

4. Choose partition count based on:
   - Expected throughput (more partitions = more parallelism)
   - Consumer count (partitions >= consumers for full parallelism)
   - Storage limits (each partition has overhead)
   - Rule of thumb: Start with 3-6 partitions, scale up if needed

5. NEVER change partition count for existing topics with keyed data:
   - hash(key) % old_count != hash(key) % new_count
   - Breaks ordering guarantees!
   - Existing keys will go to different partitions
    """)


if __name__ == "__main__":
    main()

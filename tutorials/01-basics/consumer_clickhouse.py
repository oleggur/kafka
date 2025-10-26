#!/usr/bin/env python3
"""
Tutorial 01: Basic Kafka Consumer with ClickHouse Integration
==============================================================

This script demonstrates:
- How to consume messages from a Kafka topic
- Basic consumer configuration (consumer groups, offset management)
- How to write consumed messages to ClickHouse for persistence and analysis

Key Kafka Concepts:
-------------------
1. CONSUMER: A client that reads records from Kafka topics
2. CONSUMER GROUP: A group of consumers that coordinate to consume a topic
   - Each partition is consumed by exactly ONE consumer in the group
   - This enables horizontal scaling (add more consumers to process faster)
3. OFFSET: The position of a message in a partition (sequential ID starting from 0)
4. OFFSET COMMIT: Saving the current read position so consumers can resume after restart
5. AUTO.OFFSET.RESET: What to do when there's no saved offset
   - 'earliest': Start from the beginning of the topic
   - 'latest': Only consume new messages
6. POLL: Fetching messages from Kafka (pull model, not push)

ClickHouse Integration:
-----------------------
Messages consumed from Kafka are inserted into ClickHouse for:
- Persistent storage beyond Kafka's retention period
- SQL analytics and aggregations
- Joining with other data sources
"""

import json
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def main() -> None:
    """
    Main consumer logic: reads messages from Kafka and writes them to ClickHouse.
    """

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================

    consumer_config: Dict[str, Any] = {
        # BOOTSTRAP.SERVERS: How to reach the Kafka cluster
        "bootstrap.servers": "localhost:9092",

        # GROUP.ID: The consumer group this consumer belongs to
        # Kafka Concept - CONSUMER GROUP:
        #   - Multiple consumers with the same group.id work together
        #   - Kafka ensures each partition is consumed by only ONE consumer in the group
        #   - If a consumer dies, its partitions are reassigned to other group members
        #   - Offsets are tracked per consumer group (different groups can read independently)
        "group.id": "basic_clickhouse_sink",

        # AUTO.OFFSET.RESET: What to do when there's no committed offset for this group
        # 'earliest': Read from the beginning of the topic (all messages)
        # 'latest': Only read new messages produced after consumer starts
        "auto.offset.reset": "earliest",

        # ENABLE.AUTO.COMMIT: Whether to automatically commit offsets periodically
        # true (default): Kafka automatically commits offsets every 5 seconds
        # false: You must manually commit offsets (gives you more control)
        # We'll use auto-commit for simplicity in this basic example
        "enable.auto.commit": True,

        # AUTO.COMMIT.INTERVAL.MS: How often to auto-commit offsets (in milliseconds)
        "auto.commit.interval.ms": 5000,  # 5 seconds
    }

    # Create the Consumer instance
    consumer: Consumer = Consumer(consumer_config)

    # ============================================================================
    # TOPIC SUBSCRIPTION
    # ============================================================================
    # SUBSCRIBE: Dynamically assigns partitions to this consumer
    # Alternative: ASSIGN (manually specify which partitions to read)
    # For most use cases, subscribe() is preferred as it handles rebalancing

    topic: str = "ads_basic"
    consumer.subscribe([topic])

    print(f"[START] Consuming from topic: '{topic}'")
    print(f"[INFO] Consumer group: {consumer_config['group.id']}")
    print(f"[INFO] Auto offset reset: {consumer_config['auto.offset.reset']}")
    print("-" * 60)

    # ============================================================================
    # CLICKHOUSE SETUP
    # ============================================================================
    # Connect to ClickHouse (docker-compose exposes HTTP interface on port 8123)

    clickhouse_client: Client = get_client(
        host="localhost",
        port=8123,
        username="default",
        password="secret",
        database="demo"
    )

    # Create the table if it doesn't exist
    # MergeTree is ClickHouse's primary table engine for analytics
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS ads_basic (
            inserted_at DateTime DEFAULT now(),  -- When the row was inserted into ClickHouse
            seq UInt64,                            -- Sequence number from Kafka message
            campaign_id String,                    -- Campaign identifier
            spend Float64,                         -- Ad spend amount
            currency String                        -- Currency code
        ) ENGINE = MergeTree()
        ORDER BY (campaign_id, seq)                -- Primary key for efficient queries
    """)

    print("[CLICKHOUSE] Connected and table 'ads_basic' ready")
    print("-" * 60)

    # ============================================================================
    # CONSUME MESSAGES
    # ============================================================================
    # POLL LOOP: The standard pattern for consuming from Kafka
    # The consumer runs in an infinite loop, polling for new messages

    message_count: int = 0

    try:
        while True:
            # POLL: Fetch messages from Kafka
            # Kafka Concept - PULL MODEL:
            #   Consumers pull messages from Kafka at their own pace
            #   (vs. push model where server pushes to clients)
            # The timeout parameter (1.0 seconds) controls how long to wait if no messages
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                # No message available within the timeout period
                # This is normal - just means Kafka has no new data right now
                continue

            # Check for errors
            if msg.error():
                # Kafka Concept - PARTITION EOF:
                #   _PARTITION_EOF means we've reached the end of a partition (not an error)
                #   This happens when reading historical data with auto.offset.reset=earliest
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition - not a real error
                    continue
                else:
                    # Real error - raise exception
                    raise KafkaException(msg.error())

            # ========================================================================
            # PROCESS MESSAGE
            # ========================================================================
            # Extract message metadata
            topic_name: str = msg.topic()
            partition: int = msg.partition()
            offset: int = msg.offset()

            # Decode the message value (bytes -> string -> JSON)
            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue  # Skip tombstone messages (null values used for deletion)

            value_str: str = value_bytes.decode("utf-8")
            data: Dict[str, Any] = json.loads(value_str)

            # Extract fields from JSON
            seq: int = int(data["seq"])
            campaign_id: str = str(data["campaign_id"])
            spend: float = float(data["spend"])
            currency: str = str(data["currency"])

            message_count += 1

            print(
                f"[RECV #{message_count}] "
                f"topic={topic_name} partition={partition} offset={offset} | "
                f"campaign={campaign_id} spend={spend} {currency}"
            )

            # ========================================================================
            # INSERT INTO CLICKHOUSE
            # ========================================================================
            # Write the consumed message to ClickHouse for persistence and analytics

            clickhouse_client.insert(
                table="ads_basic",
                data=[(seq, campaign_id, spend, currency)],
                column_names=["seq", "campaign_id", "spend", "currency"]
            )

            print(f"[CLICKHOUSE] Inserted seq={seq} into ads_basic table")

    except KeyboardInterrupt:
        # User pressed Ctrl+C to stop the consumer
        print("\n" + "-" * 60)
        print(f"[STOP] Shutting down consumer... (processed {message_count} messages)")

    finally:
        # ========================================================================
        # CLEANUP
        # ========================================================================
        # Kafka Concept - GRACEFUL SHUTDOWN:
        #   consumer.close() commits final offsets and leaves the consumer group
        #   This triggers a rebalance so other consumers can take over partitions
        #   Always call close() to avoid consumer group instability

        consumer.close()
        print("[CONSUMER] Closed cleanly")

        # Show ClickHouse data summary
        result = clickhouse_client.query("SELECT COUNT(*) as count FROM ads_basic")
        total_rows = result.result_rows[0][0]
        print(f"[CLICKHOUSE] Total rows in ads_basic: {total_rows}")


if __name__ == "__main__":
    main()

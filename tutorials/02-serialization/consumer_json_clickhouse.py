#!/usr/bin/env python3
"""
Tutorial 02-A: JSON Consumer with ClickHouse
============================================

This script demonstrates:
- Consuming JSON-encoded messages from Kafka
- Handling schema variations in JSON (missing fields, extra fields)
- Storing JSON data in ClickHouse

Key Concepts:
-------------
1. JSON DESERIALIZATION: Converting JSON bytes back to Python objects
2. SCHEMA FLEXIBILITY: JSON's lack of schema enforcement
3. ERROR HANDLING: Dealing with missing/invalid fields
"""

import json
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def main() -> None:
    """
    Consume JSON messages and write to ClickHouse with proper error handling.
    """

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "json_clickhouse_sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    consumer: Consumer = Consumer(consumer_config)
    topic: str = "ads_json"
    consumer.subscribe([topic])

    print(f"[START] Consuming JSON messages from topic: '{topic}'")
    print("-" * 60)

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

    # Create table with nullable fields to handle missing data
    # Kafka Concept - SCHEMA EVOLUTION:
    #   When using JSON, your consumer must handle schema changes gracefully
    #   ClickHouse Nullable types allow us to insert NULL when fields are missing
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS ads_json (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Nullable(Float64),          -- Nullable to handle missing values
            currency String,
            timestamp Nullable(String),        -- Store ISO timestamp as string
            metadata_source Nullable(String),  -- Flattened nested field
            metadata_version Nullable(String), -- Flattened nested field
            tags Array(String) DEFAULT []      -- Array type for tags
        ) ENGINE = MergeTree()
        ORDER BY (campaign_id, seq)
    """)

    print("[CLICKHOUSE] Table 'ads_json' ready")
    print("-" * 60)

    # ============================================================================
    # CONSUME AND PROCESS MESSAGES
    # ============================================================================

    message_count: int = 0
    error_count: int = 0

    try:
        while True:
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # ========================================================================
            # DESERIALIZE JSON
            # ========================================================================
            value_bytes: Optional[bytes] = msg.value()
            if value_bytes is None:
                continue

            try:
                # DESERIALIZATION: bytes -> string -> Python dict
                value_str: str = value_bytes.decode("utf-8")
                data: Dict[str, Any] = json.loads(value_str)

                # ====================================================================
                # EXTRACT FIELDS WITH DEFENSIVE CODING
                # ====================================================================
                # JSON Pitfall: Fields might be missing or have wrong types
                # We use .get() with defaults to handle missing fields gracefully

                seq: int = int(data.get("seq", 0))
                campaign_id: str = str(data.get("campaign_id", "unknown"))

                # Handle missing or invalid spend field
                spend: Optional[float] = None
                if "spend" in data:
                    try:
                        spend = float(data["spend"])
                    except (ValueError, TypeError):
                        # If spend is not a valid number, set to None
                        print(f"[WARNING] Invalid spend value: {data['spend']}")
                        error_count += 1

                currency: str = str(data.get("currency", "USD"))
                timestamp: Optional[str] = data.get("timestamp")

                # Extract nested metadata fields (if present)
                metadata: Dict[str, Any] = data.get("metadata", {})
                metadata_source: Optional[str] = metadata.get("source")
                metadata_version: Optional[str] = metadata.get("version")

                # Extract tags array (default to empty list if missing)
                tags: list[str] = data.get("tags", [])

                message_count += 1

                print(f"[RECV #{message_count}] partition={msg.partition()} offset={msg.offset()}")
                print(f"  campaign_id: {campaign_id}")
                print(f"  spend: {spend}")
                print(f"  currency: {currency}")
                print(f"  timestamp: {timestamp}")
                print(f"  tags: {tags}")

                # ====================================================================
                # INSERT INTO CLICKHOUSE
                # ====================================================================
                clickhouse_client.insert(
                    table="ads_json",
                    data=[(
                        seq,
                        campaign_id,
                        spend,  # Can be None (NULL in ClickHouse)
                        currency,
                        timestamp,  # Can be None
                        metadata_source,  # Can be None
                        metadata_version,  # Can be None
                        tags
                    )],
                    column_names=[
                        "seq",
                        "campaign_id",
                        "spend",
                        "currency",
                        "timestamp",
                        "metadata_source",
                        "metadata_version",
                        "tags"
                    ]
                )

                print(f"[CLICKHOUSE] Inserted seq={seq}")

            except json.JSONDecodeError as e:
                # Handle invalid JSON
                print(f"[ERROR] Failed to decode JSON: {e}")
                print(f"  Raw value: {value_bytes}")
                error_count += 1

            except Exception as e:
                # Catch any other errors during processing
                print(f"[ERROR] Failed to process message: {e}")
                print(f"  Data: {data if 'data' in locals() else 'N/A'}")
                error_count += 1

            print()

    except KeyboardInterrupt:
        print("-" * 60)
        print(f"[STOP] Shutting down...")
        print(f"  Messages processed: {message_count}")
        print(f"  Errors encountered: {error_count}")

    finally:
        consumer.close()
        print("[CONSUMER] Closed")

        # Show data summary
        result = clickhouse_client.query("SELECT COUNT(*) FROM ads_json")
        total_rows = result.result_rows[0][0]
        print(f"[CLICKHOUSE] Total rows in ads_json: {total_rows}")

        # Show some example queries
        print("\n" + "=" * 60)
        print("EXAMPLE CLICKHOUSE QUERIES")
        print("=" * 60)
        print("\n1. Total spend by campaign:")
        print("   SELECT campaign_id, SUM(spend) FROM ads_json GROUP BY campaign_id;")
        print("\n2. Messages with missing spend:")
        print("   SELECT COUNT(*) FROM ads_json WHERE spend IS NULL;")
        print("\n3. Messages with specific tags:")
        print("   SELECT * FROM ads_json WHERE has(tags, 'analytics');")


if __name__ == "__main__":
    main()

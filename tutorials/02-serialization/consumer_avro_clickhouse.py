#!/usr/bin/env python3
"""
Tutorial 02-B: Avro Consumer with ClickHouse
============================================

This script demonstrates:
- Consuming Avro-encoded messages from Kafka
- Automatic schema fetching from Schema Registry
- Type-safe deserialization
- Storing Avro data in ClickHouse

Key Concepts:
-------------
1. AVRO DESERIALIZATION: Binary -> Python objects using schema
2. SCHEMA REGISTRY: Consumer fetches schema by ID from messages
3. SCHEMA CACHING: Schema Registry client caches schemas locally
4. WIRE FORMAT: Confluent format [magic_byte][schema_id][avro_data]

How Avro Consumer Works:
------------------------
1. Consumer reads message bytes from Kafka
2. AvroDeserializer reads schema_id from message header
3. Deserializer fetches schema from Schema Registry (if not cached)
4. Deserializer decodes Avro binary data using that schema
5. Returns a Python dict with correct types
"""

from typing import Dict, Any, Optional

from confluent_kafka import DeserializingConsumer, KafkaException, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client

# Note: DeserializingConsumer is marked "experimental" in the library docs,
# but it's the recommended approach in Confluent's official documentation
# and is widely used in production. It simplifies Avro deserialization.


def main() -> None:
    """
    Consume Avro messages and write to ClickHouse.
    """

    # ============================================================================
    # SCHEMA REGISTRY SETUP
    # ============================================================================

    schema_registry_config: Dict[str, str] = {
        "url": "http://localhost:8081"
    }

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(
        schema_registry_config
    )

    print("[SCHEMA REGISTRY] Connected to http://localhost:8081")

    # ============================================================================
    # CREATE DESERIALIZERS
    # ============================================================================

    # AvroDeserializer automatically:
    # 1. Reads schema_id from message header
    # 2. Fetches schema from Schema Registry (with caching)
    # 3. Deserializes Avro binary to Python dict
    avro_deserializer: AvroDeserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client
    )

    # StringDeserializer for keys
    string_deserializer: StringDeserializer = StringDeserializer("utf-8")

    # ============================================================================
    # CONSUMER CONFIGURATION
    # ============================================================================
    # DeserializingConsumer integrates deserializers into the consumer
    # It automatically deserializes keys and values after receiving

    consumer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "avro_clickhouse_sink",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        # Deserializers are configured here (not in poll() call)
        "key.deserializer": string_deserializer,
        "value.deserializer": avro_deserializer,
    }

    consumer: DeserializingConsumer = DeserializingConsumer(consumer_config)
    topic: str = "ads_avro"
    consumer.subscribe([topic])

    print(f"[START] Consuming Avro messages from topic: '{topic}'")
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

    # Create table matching Avro schema
    # Since Avro enforces types, we can use non-nullable columns
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS ads_avro (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Float64,                     -- Not nullable (Avro ensures value exists)
            currency String,
            timestamp String,                  -- ISO 8601 string
            metadata_source String,            -- Flattened nested field
            metadata_version String,           -- Flattened nested field
            tags Array(String)                 -- Array type
        ) ENGINE = MergeTree()
        ORDER BY (campaign_id, seq)
    """)

    print("[CLICKHOUSE] Table 'ads_avro' ready")
    print("-" * 60)

    # ============================================================================
    # CONSUME AND PROCESS MESSAGES
    # ============================================================================

    message_count: int = 0

    try:
        while True:
            # Poll returns a Message object
            # DeserializingConsumer automatically deserializes key() and value()
            msg: Optional[Message] = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # ========================================================================
            # EXTRACT DESERIALIZED DATA
            # ========================================================================
            # Unlike raw Consumer, DeserializingConsumer returns Python objects
            # msg.value() is already a dict (deserialized from Avro)
            # msg.key() is already a string (deserialized from bytes)

            key: Optional[str] = msg.key()
            data: Optional[Dict[str, Any]] = msg.value()

            if data is None:
                continue

            # With Avro, we have TYPE SAFETY
            # - All required fields are guaranteed to exist
            # - All fields have the correct type
            # - No need for .get() with defaults or try/except for type errors

            seq: int = int(data["seq"])
            campaign_id: str = str(data["campaign_id"])
            spend: float = float(data["spend"])  # Guaranteed to be a number
            currency: str = str(data["currency"])
            timestamp: str = str(data["timestamp"])

            # Extract nested metadata
            metadata: Dict[str, str] = data["metadata"]
            metadata_source: str = metadata["source"]
            metadata_version: str = metadata["version"]

            # Extract tags array
            tags: list[str] = data["tags"]

            message_count += 1

            print(f"[RECV #{message_count}] partition={msg.partition()} offset={msg.offset()}")
            print(f"  seq: {seq} (type: {type(seq).__name__})")
            print(f"  campaign_id: {campaign_id}")
            print(f"  spend: {spend} (type: {type(spend).__name__})")
            print(f"  currency: {currency}")
            print(f"  timestamp: {timestamp}")
            print(f"  metadata: source={metadata_source}, version={metadata_version}")
            print(f"  tags: {tags}")

            # ========================================================================
            # INSERT INTO CLICKHOUSE
            # ========================================================================
            clickhouse_client.insert(
                table="ads_avro",
                data=[(
                    seq,
                    campaign_id,
                    spend,
                    currency,
                    timestamp,
                    metadata_source,
                    metadata_version,
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
            print()

    except KeyboardInterrupt:
        print("-" * 60)
        print(f"[STOP] Shutting down...")
        print(f"  Messages processed: {message_count}")

    finally:
        consumer.close()
        print("[CONSUMER] Closed")

        # Show data summary
        result = clickhouse_client.query("SELECT COUNT(*) FROM ads_avro")
        total_rows = result.result_rows[0][0]
        print(f"[CLICKHOUSE] Total rows in ads_avro: {total_rows}")

        # ========================================================================
        # AVRO vs JSON COMPARISON
        # ========================================================================
        print("\n" + "=" * 60)
        print("AVRO vs JSON COMPARISON")
        print("=" * 60)
        print("\nAVRO Benefits Demonstrated:")
        print("  1. TYPE SAFETY - No runtime type errors (spend is always float)")
        print("  2. REQUIRED FIELDS - All fields guaranteed to exist")
        print("  3. COMPACT FORMAT - Binary encoding is smaller than JSON")
        print("  4. SCHEMA EVOLUTION - Schema Registry manages compatibility")
        print("\nJSON Benefits:")
        print("  1. HUMAN READABLE - Can inspect messages with text tools")
        print("  2. FLEXIBLE - Easy to add/remove fields without coordination")
        print("  3. NO INFRASTRUCTURE - No Schema Registry needed")
        print("  4. SIMPLE - Just json.dumps() and json.loads()")
        print("\nUse Avro when:")
        print("  - Performance matters (high throughput)")
        print("  - Schema evolution is planned")
        print("  - Type safety is important")
        print("\nUse JSON when:")
        print("  - Debugging/development")
        print("  - Rapid prototyping")
        print("  - External systems need human-readable format")


if __name__ == "__main__":
    main()

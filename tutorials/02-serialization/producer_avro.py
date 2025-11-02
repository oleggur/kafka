#!/usr/bin/env python3
"""
Tutorial 02-B: Avro Serialization with Schema Registry
======================================================

This script demonstrates:
- Avro binary serialization (compact, fast)
- Schema Registry for centralized schema management
- Schema validation and evolution
- Type safety vs JSON

Key Concepts:
-------------
1. AVRO: A binary serialization format developed by Apache
2. SCHEMA REGISTRY: A service that stores and versions schemas
3. SCHEMA ID: Each schema gets a unique ID; messages reference this ID
4. SCHEMA EVOLUTION: Rules for safely changing schemas over time
5. WIRE FORMAT: Confluent's format: [magic_byte][schema_id][avro_data]

Avro Pros:
- Compact binary format (smaller messages than JSON)
- Fast serialization/deserialization
- Schema enforcement (catches errors at write time)
- Built-in schema evolution support
- Type safety (field types are enforced)

Avro Cons:
- Not human-readable (can't inspect with cat/less)
- Requires Schema Registry infrastructure
- Less flexible than JSON (schema changes need planning)
- Schema must be defined upfront

How Schema Registry Works:
--------------------------
1. Producer registers schema with Schema Registry
2. Schema Registry assigns a unique ID
3. Producer serializes data using that schema
4. Producer writes: [magic_byte][schema_id][avro_binary_data]
5. Consumer reads schema_id from message
6. Consumer fetches schema from Schema Registry (cached)
7. Consumer deserializes using that schema
"""

import random
import time
from typing import Dict, Any, Optional
from datetime import datetime

from confluent_kafka import SerializingProducer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# Note: SerializingProducer is marked "experimental" in the library docs,
# but it's the recommended approach in Confluent's official documentation
# and is widely used in production. It simplifies Avro serialization.


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def main() -> None:
    """
    Produce Avro-encoded messages using Schema Registry.
    """

    # ============================================================================
    # SCHEMA REGISTRY SETUP
    # ============================================================================
    # Schema Registry is a separate service that manages Avro schemas
    # It provides:
    # - Centralized schema storage
    # - Schema versioning
    # - Compatibility checking (prevents breaking changes)

    schema_registry_config: Dict[str, str] = {
        "url": "http://localhost:8081"  # Schema Registry REST API
    }

    schema_registry_client: SchemaRegistryClient = SchemaRegistryClient(
        schema_registry_config
    )

    print("[SCHEMA REGISTRY] Connected to http://localhost:8081")

    # ============================================================================
    # DEFINE AVRO SCHEMA
    # ============================================================================
    # Avro schemas are defined in JSON format
    # This schema defines the structure and types of our messages

    # Kafka Concept - SCHEMA SUBJECTS:
    #   Subject = topic name + '-key' or '-value'
    #   Example: 'ads_avro-value' for the value schema of topic 'ads_avro'
    #   Each subject can have multiple versions (schema evolution)

    avro_schema_str: str = """
    {
        "type": "record",
        "name": "AdEvent",
        "namespace": "com.example.kafka.tutorial",
        "doc": "Schema for ad spend events",
        "fields": [
            {
                "name": "seq",
                "type": "long",
                "doc": "Sequence number of this event"
            },
            {
                "name": "campaign_id",
                "type": "string",
                "doc": "Unique identifier for the campaign"
            },
            {
                "name": "spend",
                "type": "double",
                "doc": "Amount spent on this ad"
            },
            {
                "name": "currency",
                "type": "string",
                "doc": "Currency code (e.g., USD, EUR)"
            },
            {
                "name": "timestamp",
                "type": "string",
                "doc": "ISO 8601 timestamp when event occurred"
            },
            {
                "name": "metadata",
                "type": {
                    "type": "record",
                    "name": "Metadata",
                    "fields": [
                        {"name": "source", "type": "string"},
                        {"name": "version", "type": "string"}
                    ]
                },
                "doc": "Nested metadata object"
            },
            {
                "name": "tags",
                "type": {
                    "type": "array",
                    "items": "string"
                },
                "doc": "Array of tags"
            }
        ]
    }
    """

    print("[SCHEMA] Avro schema defined:")
    print(avro_schema_str)
    print("-" * 60)

    # ============================================================================
    # CREATE SERIALIZERS
    # ============================================================================

    # AvroSerializer automatically:
    # 1. Registers the schema with Schema Registry (if not already registered)
    # 2. Serializes Python dicts to Avro binary format
    # 3. Prepends the Confluent wire format header (magic byte + schema ID)
    avro_serializer: AvroSerializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=avro_schema_str
    )

    # Use string serializer for keys (campaign_id)
    string_serializer: StringSerializer = StringSerializer("utf-8")

    # ============================================================================
    # PRODUCER CONFIGURATION
    # ============================================================================
    # SerializingProducer integrates serializers into the producer
    # It automatically serializes keys and values before sending

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,
        # Serializers are configured here (not in produce() call)
        "key.serializer": string_serializer,
        "value.serializer": avro_serializer,
    }

    producer: SerializingProducer = SerializingProducer(producer_config)
    topic: str = "ads_avro"

    print(f"[START] Producing Avro messages to topic: '{topic}'")
    print("-" * 60)

    # ============================================================================
    # PRODUCE MESSAGES
    # ============================================================================

    num_messages: int = 10

    for seq in range(num_messages):
        # Create event as a Python dict
        # The dict MUST match the Avro schema exactly
        # - All required fields must be present
        # - Types must match (int/long, float/double, str, etc.)
        # - Nested objects must match nested record schemas

        event: Dict[str, Any] = {
            "seq": seq,  # Must be int/long
            "campaign_id": f"campaign_{seq % 3}",  # Must be string
            "spend": round(random.uniform(10.0, 100.0), 2),  # Must be float/double
            "currency": "USD",  # Must be string
            "timestamp": datetime.utcnow().isoformat(),  # Must be string
            "metadata": {  # Must match nested Metadata record
                "source": "tutorial",
                "version": "1.0"
            },
            "tags": ["analytics", "ads"]  # Must be array of strings
        }

        # Key for partitioning
        key: str = event["campaign_id"]

        try:
            # SerializingProducer automatically serializes key and value
            # No need to manually call encode() or json.dumps()
            producer.produce(
                topic=topic,
                key=key,  # String (will be serialized by string_serializer)
                value=event,  # Dict (will be serialized by avro_serializer)
                on_delivery=delivery_report
            )

            producer.poll(0)

            print(f"[SENT #{seq}] {event}")

        except Exception as e:
            # Avro serializer will raise an exception if data doesn't match schema
            print(f"[ERROR] Failed to serialize message: {e}")
            print(f"  Event: {event}")

        time.sleep(0.2)

    # ============================================================================
    # DEMONSTRATE AVRO VALIDATION
    # ============================================================================
    print("-" * 60)
    print("[DEMO] Avro Schema Validation")
    print("-" * 60)

    # Try to send a message that violates the schema
    # This will FAIL at serialization time (before sending to Kafka)

    invalid_events = [
        {
            "desc": "Missing required field",
            "event": {
                "seq": 100,
                "campaign_id": "campaign_invalid",
                # "spend" is missing! (required field)
                "currency": "USD",
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": {"source": "tutorial", "version": "1.0"},
                "tags": []
            }
        },
        {
            "desc": "Wrong type for spend",
            "event": {
                "seq": 101,
                "campaign_id": "campaign_invalid",
                "spend": "not a number",  # Should be double!
                "currency": "USD",
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": {"source": "tutorial", "version": "1.0"},
                "tags": []
            }
        }
    ]

    for item in invalid_events:
        try:
            producer.produce(
                topic=topic,
                key=item["event"]["campaign_id"],
                value=item["event"],
                on_delivery=delivery_report
            )
            print(f"[UNEXPECTED] {item['desc']} - should have failed!")
        except Exception as e:
            # This is EXPECTED - Avro catches schema violations
            print(f"[EXPECTED ERROR] {item['desc']}")
            print(f"  Error: {type(e).__name__}: {e}")

    print("-" * 60)
    print("[SUCCESS] Avro prevented invalid messages from being sent!")
    print("-" * 60)

    # Flush all messages
    remaining: int = producer.flush(timeout=10)

    if remaining == 0:
        print(f"[SUCCESS] All valid messages delivered")
    else:
        print(f"[WARNING] {remaining} messages not delivered")

    # ============================================================================
    # SCHEMA REGISTRY INFO
    # ============================================================================
    print("\n" + "=" * 60)
    print("SCHEMA REGISTRY")
    print("=" * 60)
    print(f"View registered schemas: http://localhost:8081/subjects")
    print(f"View this schema: http://localhost:8081/subjects/ads_avro-value/versions/latest")


if __name__ == "__main__":
    main()

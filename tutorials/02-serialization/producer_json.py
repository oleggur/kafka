#!/usr/bin/env python3
"""
Tutorial 02-A: JSON Serialization
==================================

This script demonstrates:
- JSON serialization for Kafka messages
- Why JSON is human-readable but has trade-offs
- Message key vs value serialization

Key Concepts:
-------------
1. SERIALIZATION: Converting data structures (dicts, objects) to bytes for transmission
2. DESERIALIZATION: Converting bytes back to data structures
3. JSON: JavaScript Object Notation - text-based, human-readable format
4. SCHEMA-LESS: JSON doesn't enforce a schema (field types can vary)

JSON Pros:
- Human-readable (easy to debug)
- Flexible schema (can add/remove fields without breaking consumers)
- Wide language support

JSON Cons:
- Verbose (larger message size than binary formats)
- No schema enforcement (typos and type errors only caught at runtime)
- Slower serialization/deserialization vs binary formats
- No built-in versioning/compatibility checks
"""

import json
import random
import time
from typing import Dict, Any, Optional
from datetime import datetime

from confluent_kafka import Producer, KafkaError


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """Callback to confirm message delivery."""
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    """
    Produce JSON-encoded messages to demonstrate serialization patterns.
    """

    # ============================================================================
    # PRODUCER CONFIGURATION
    # ============================================================================

    producer_config: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,
    }

    producer: Producer = Producer(producer_config)
    topic: str = "ads_json"

    print("[START] Producing JSON messages")
    print(f"[TOPIC] {topic}")
    print("-" * 60)

    # ============================================================================
    # PRODUCE MESSAGES WITH JSON SERIALIZATION
    # ============================================================================

    num_messages: int = 10

    for seq in range(num_messages):
        # Create a sample event with various data types
        event: Dict[str, Any] = {
            "seq": seq,
            "campaign_id": f"campaign_{seq % 3}",
            "spend": round(random.uniform(10.0, 100.0), 2),
            "currency": "USD",
            "timestamp": datetime.utcnow().isoformat(),  # ISO 8601 format
            "metadata": {
                # Nested objects are supported by JSON
                "source": "tutorial",
                "version": "1.0"
            },
            "tags": ["analytics", "ads"]  # Arrays are supported
        }

        # KEY SERIALIZATION:
        # We use campaign_id as the key for partitioning
        # Keys must be serialized to bytes (here we use UTF-8 encoding)
        key: str = event["campaign_id"]
        key_bytes: bytes = key.encode("utf-8")

        # VALUE SERIALIZATION:
        # Convert Python dict -> JSON string -> UTF-8 bytes
        # Kafka Concept - MESSAGE FORMAT:
        #   Kafka stores all messages as raw bytes
        #   We must serialize (encode) our data before sending
        #   Consumers must deserialize (decode) after receiving
        value_str: str = json.dumps(event)
        value_bytes: bytes = value_str.encode("utf-8")

        # Send the serialized message
        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=delivery_report
        )

        producer.poll(0)

        # Print both the Python dict and its JSON representation
        print(f"[SENT #{seq}]")
        print(f"  Python: {event}")
        print(f"  JSON:   {value_str}")
        print(f"  Bytes:  {len(value_bytes)} bytes")
        print()

        time.sleep(0.2)

    # ============================================================================
    # DEMONSTRATE JSON FLEXIBILITY (AND PITFALLS)
    # ============================================================================
    print("-" * 60)
    print("[DEMO] JSON Schema Flexibility")
    print("-" * 60)

    # JSON allows schema changes without coordination between producer/consumer
    # This is both a feature (flexibility) and a bug (no validation)

    # Message with EXTRA field (forward compatibility)
    extra_field_event = {
        "seq": 100,
        "campaign_id": "campaign_extra",
        "spend": 50.0,
        "currency": "USD",
        "new_field": "This didn't exist in earlier messages!"  # NEW FIELD
    }

    # Message with MISSING field (backward compatibility)
    missing_field_event = {
        "seq": 101,
        "campaign_id": "campaign_missing",
        # "spend" field is missing!
        "currency": "USD"
    }

    # Message with WRONG TYPE (no validation!)
    wrong_type_event = {
        "seq": 102,
        "campaign_id": "campaign_wrong",
        "spend": "this should be a number!",  # WRONG TYPE (string instead of float)
        "currency": "USD"
    }

    for event in [extra_field_event, missing_field_event, wrong_type_event]:
        key_bytes = event["campaign_id"].encode("utf-8")
        value_bytes = json.dumps(event).encode("utf-8")

        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            on_delivery=delivery_report
        )
        producer.poll(0)

        print(f"[SENT] {event}")

    print("-" * 60)
    print("[WARNING] JSON allowed all these schema violations!")
    print("  - Extra fields: Consumers may ignore them")
    print("  - Missing fields: Consumers will get KeyError")
    print("  - Wrong types: Consumers will get type errors")
    print("-" * 60)

    # Flush all messages
    remaining: int = producer.flush(timeout=10)

    if remaining == 0:
        print(f"[SUCCESS] All messages delivered")
    else:
        print(f"[WARNING] {remaining} messages not delivered")


if __name__ == "__main__":
    main()

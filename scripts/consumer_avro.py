#!/usr/bin/env python3
"""
Day 2 - Phase B: Avro Consumer with Schema Registry (typed, fixed)
- Joins group 'g_avro_demo'
- Subscribes to 'ads_raw_avro'
- Automatically deserializes Avro values using Schema Registry
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from confluent_kafka import KafkaException, KafkaError, Message
from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer


# ---- Kafka & Registry configuration ----
# 'bootstrap.servers' -> how to reach Kafka (host listener exposed in docker-compose)
# 'group.id'          -> consumer group id (offset tracking & rebalancing)
# 'auto.offset.reset' -> if the group has no offsets yet: 'earliest' reads from start
kafka_conf: Dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "g_avro_demo",
    "auto.offset.reset": "earliest",
}

# Schema Registry REST API (exposed in docker-compose at localhost:8081)
schema_registry_conf: Dict[str, Any] = {
    "url": "http://localhost:8081",
}

# Create the SR client once and reuse it (handles schema id lookups/caching)
sr_client: SchemaRegistryClient = SchemaRegistryClient(schema_registry_conf)

# Key deserializer: incoming bytes -> Python str (UTF-8)
key_deserializer: StringDeserializer = StringDeserializer("utf_8")

# Value deserializer: Avro bytes -> Python dict
# NOTE: The correct keyword is 'schema_registry_client', not 'sr_client'
# - We don't pass a schema string: the deserializer fetches by schema id embedded in the message
# - We don't pass 'from_dict': default returns a dict matching the writer schema
value_deserializer: AvroDeserializer = AvroDeserializer(
    schema_registry_client=sr_client
)

# Build the consumer with our deserializers
consumer: DeserializingConsumer = DeserializingConsumer(
    {
        **kafka_conf,
        "key.deserializer": key_deserializer,
        "value.deserializer": value_deserializer,
    }
)


def main() -> None:
    topic: str = "ads_raw_avro"
    consumer.subscribe([topic])  # join group & subscribe (enables rebalances)

    print("[CONSUMER-AVRO] waiting for messages… Ctrl+C to stop.")
    try:
        while True:
            # poll() returns a Message or None if no data within the timeout
            msg: Optional[Message] = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                # _PARTITION_EOF isn't an error; it means we've reached the current end
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # anything else: raise so you see it
                raise KafkaException(msg.error())

            # After deserialization:
            # - key: Optional[str]
            # - value: dict[str, Any] conforming to the registered Avro schema
            key: Optional[str] = msg.key()
            value: Any = msg.value()

            print(
                f"[RECV-AVRO] topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()} key={key} value={value}"
            )

    except KeyboardInterrupt:
        print("\n[CONSUMER-AVRO] stopping…")
    finally:
        # Close commits offsets (if auto-commit enabled) and leaves the group cleanly
        consumer.close()
        print("[CONSUMER-AVRO] closed.")


if __name__ == "__main__":
    main()

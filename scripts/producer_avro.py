#!/usr/bin/env python3
"""
Day 2 - Phase B: Avro Producer with Schema Registry (typed)
- Connects to Kafka (localhost:9092)
- Connects to Schema Registry (localhost:8081)
- Registers an Avro schema (subject: 'ads_raw_avro-value')
- Produces Avro-encoded records to topic 'ads_raw_avro'
"""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict

from confluent_kafka import Message  # used in delivery callback
from confluent_kafka import KafkaError
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer


# A precise type for our value records
class AdEvent(TypedDict):
    campaign_id: str
    spend: float
    currency: str


# ---- Kafka & Registry configuration ----
kafka_conf: Dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",  # host listener exposed in docker-compose
}

schema_registry_conf: Dict[str, Any] = {
    "url": "http://localhost:8081",        # Schema Registry REST API exposed in docker-compose
}

# Create the Schema Registry client (handles subject lookup, id caching, etc.)
sr_client: SchemaRegistryClient = SchemaRegistryClient(schema_registry_conf)

# Define an Avro schema for the *value* of our messages (keys can have their own schema; we'll keep key as plain string)
value_schema_str: str = """
{
  "type": "record",
  "name": "AdEvent",
  "namespace": "demo",
  "fields": [
    {"name": "campaign_id", "type": "string"},
    {"name": "spend",       "type": "double"},
    {"name": "currency",    "type": "string"}
  ]
}
"""

# Wrap the raw schema string in a Schema object (type='AVRO'). Helpful when you want to register manually.
value_schema: Schema = Schema(value_schema_str, "AVRO")

# Create an AvroSerializer bound to that schema + registry.
# - It registers (if needed) under subject 'ads_raw_avro-value'
# - It writes the Confluent wire-format header with schema id
avro_value_serializer: AvroSerializer = AvroSerializer(sr_client, value_schema_str)

# Use a simple string serializer for the message key (we’ll use campaign_id)
string_key_serializer: StringSerializer = StringSerializer("utf_8")

# Build a SerializingProducer which knows how to serialize keys/values per message
producer: SerializingProducer = SerializingProducer(
    {
        **kafka_conf,
        "key.serializer": string_key_serializer,     # serializer for keys (str -> bytes)
        "value.serializer": avro_value_serializer,   # serializer for values (dict -> Avro bytes via registry)
        # you could also set "linger.ms", "enable.idempotence", etc. here
    }
)


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Called on each message delivery attempt (success or failure)."""
    if err is not None:
        print(f"[DELIVERY-ERROR] {err}")
    else:
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    topic: str = "ads_raw_avro"
    # Some sample records (must validate against our Avro schema above)
    records: List[AdEvent] = [
        {"campaign_id": "123", "spend": 10.5, "currency": "USD"},
        {"campaign_id": "123", "spend": 7.25, "currency": "USD"},
        {"campaign_id": "456", "spend": 3.10, "currency": "USD"},
        {"campaign_id": "789", "spend": 42.0, "currency": "USD"},
    ]

    for r in records:
        # key is the campaign_id string (keeps all same-campaign messages in one partition)
        key: str = r["campaign_id"]

        # value is a dict that AvroSerializer will validate against the schema and encode
        producer.produce(topic=topic, key=key, value=r, on_delivery=delivery_report)
        producer.poll(0)  # serve delivery callbacks

    producer.flush(10)
    print("[FLUSHED] all Avro messages sent.")


if __name__ == "__main__":
    main()

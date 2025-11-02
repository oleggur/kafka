#!/usr/bin/env python3
"""
Tutorial 01: Basic Kafka Producer
==================================

This is the simplest possible Kafka producer. It demonstrates:
- How to connect to a Kafka broker
- How to send messages to a topic
- Basic producer configuration
- Delivery callbacks to confirm message delivery

Key Kafka Concepts:
-------------------
1. PRODUCER: A client that publishes (writes) records to Kafka topics
2. TOPIC: A category/feed name to which records are published (like a table in a database)
3. BOOTSTRAP.SERVERS: Initial list of Kafka brokers to connect to for cluster discovery
4. PRODUCE: The act of sending a message to Kafka
5. DELIVERY CALLBACK: A function called when Kafka confirms message receipt (or errors)
6. FLUSH: Ensures all buffered messages are sent before the producer closes

Message Format:
--------------
Each message contains:
- KEY: Used for partitioning (messages with same key go to same partition)
- VALUE: The actual message payload
- TOPIC: Where the message should be published
"""

import json
import random
import time
from typing import Dict, Any, Optional

from confluent_kafka import Producer, KafkaError


def delivery_report(err: Optional[KafkaError], msg) -> None:
    """
    Delivery callback: Called once for each message produced to indicate success or failure.

    This is an ASYNCHRONOUS callback - Kafka calls it after the broker acknowledges the message.

    Args:
        err: None if successful, otherwise a KafkaError
        msg: Message object containing metadata (topic, partition, offset)

    Kafka Concept - ACKNOWLEDGMENT (ACK):
        When a broker receives a message, it sends an acknowledgment back to the producer.
        This callback is invoked when that ack is received (or times out).
    """
    if err is not None:
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        # msg.topic(): The topic name where the message was written
        # msg.partition(): Which partition within the topic (0, 1, 2, ...)
        # msg.offset(): The position of this message in that partition (sequential ID)
        print(
            f"[DELIVERED] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )


def main() -> None:
    """
    Main producer logic: connects to Kafka and sends simple ad spend events.
    """

    # ============================================================================
    # PRODUCER CONFIGURATION
    # ============================================================================
    # Configuration is passed as a dictionary of key-value pairs.
    # The confluent-kafka library uses librdkafka under the hood, so config
    # parameters follow librdkafka naming conventions.

    producer_config: Dict[str, Any] = {
        # BOOTSTRAP.SERVERS: Comma-separated list of Kafka broker addresses
        # This is how the producer initially connects to the cluster
        # Format: "host1:port1,host2:port2,..."
        # Our docker-compose exposes Kafka on localhost:9092
        "bootstrap.servers": "localhost:9092",

        # LINGER.MS: How long to wait before sending a batch of messages (in milliseconds)
        # Higher values = better batching/throughput, but higher latency
        # Lower values = lower latency, but less efficient batching
        # For demos we keep it low for quick feedback
        "linger.ms": 5,
    }

    # Create the Producer instance
    # This object is thread-safe and can be reused for multiple produce() calls
    producer: Producer = Producer(producer_config)

    # ============================================================================
    # TOPIC SETUP
    # ============================================================================
    # TOPIC: A named stream of records. Think of it like a log file or database table.
    # Our docker-compose has auto.create.topics.enable=true, so topics are created
    # automatically when first used. In production, you'd typically pre-create topics.

    topic: str = "ads_basic"

    print(f"[START] Producing messages to topic: '{topic}'")
    print(f"[INFO] Kafka broker: localhost:9092")
    print("-" * 60)

    # ============================================================================
    # PRODUCE MESSAGES
    # ============================================================================
    # We'll generate 10 simple ad spend events and send them to Kafka

    num_messages: int = 10

    for seq in range(num_messages):
        # Create a sample event (dictionary representing an ad spend)
        event: Dict[str, Any] = {
            "seq": seq,                                    # Sequence number
            "campaign_id": f"campaign_{seq % 3}",         # Rotate between 3 campaigns
            "spend": round(random.uniform(10.0, 100.0), 2),  # Random spend amount
            "currency": "USD",
        }

        # KEY: We use campaign_id as the key
        # Kafka Concept - PARTITIONING BY KEY:
        #   Messages with the same key always go to the same partition
        #   This ensures ordering for messages with the same key
        #   Keys are hashed to determine partition: hash(key) % num_partitions
        key: str = event["campaign_id"]

        # VALUE: The actual message payload (JSON-encoded)
        value: str = json.dumps(event)

        # PRODUCE: Send the message to Kafka
        # This is ASYNCHRONOUS - the message is buffered and sent in the background
        producer.produce(
            topic=topic,                      # Destination topic
            key=key.encode("utf-8"),         # Key as bytes (used for partitioning)
            value=value.encode("utf-8"),     # Value as bytes (the actual message)
            on_delivery=delivery_report,      # Callback when delivery confirmed
        )

        # POLL: Trigger delivery report callbacks
        # producer.poll(timeout) processes delivery callbacks from background threads
        #
        # timeout parameter (in seconds):
        # - poll(0): Non-blocking - process ready callbacks and return immediately
        # - poll(0.1): Block up to 0.1 seconds waiting for callbacks
        # - poll(1): Block up to 1 second waiting for callbacks
        #
        # For async sending (like here), poll(0) is common - just trigger callbacks
        # For blocking/sync behavior, use poll(timeout) after each produce()
        producer.poll(0)

        print(f"[SENT] Message #{seq}: {value}")

        # Small delay for readability (not required in production)
        time.sleep(0.2)

    # ============================================================================
    # FLUSH: Wait for all messages to be delivered
    # ============================================================================
    # Kafka Concept - BUFFERING:
    #   The producer buffers messages internally for efficient batching
    #   flush() blocks until all buffered messages are sent and acknowledged
    #   The parameter is a timeout in seconds (None = wait forever)

    print("-" * 60)
    print("[FLUSHING] Waiting for all messages to be delivered...")

    remaining: int = producer.flush(timeout=10)

    if remaining > 0:
        print(f"[WARNING] {remaining} messages were not delivered within timeout")
    else:
        print(f"[SUCCESS] All {num_messages} messages delivered successfully!")


if __name__ == "__main__":
    main()

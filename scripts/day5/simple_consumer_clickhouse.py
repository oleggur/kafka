"""
Simple Kafka consumer that reads events from Kafka and writes them into ClickHouse.

Expected schema (JSON from producer):
  - seq (int)
  - campaign_id (str)
  - spend (float)
  - currency (str)

These fields are written into ClickHouse table `ads_simple`.
"""

import json
from confluent_kafka import Consumer, KafkaException
from clickhouse_connect import get_client


def main() -> None:
    # Kafka consumer config
    conf: dict[str, str] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "g_clickhouse_sink",  # consumer group ID for offset tracking
        "auto.offset.reset": "earliest",  # if no offsets saved, start from beginning
    }

    consumer: Consumer = Consumer(conf)

    topic: str = "ads_simple"
    consumer.subscribe([topic])

    # Connect to ClickHouse (Docker Compose exposes 8123 HTTP interface)
    client = get_client(host="localhost", port=8123, username="default", password="secret", database="demo")

    # Make sure the table exists
    client.command("""
        CREATE TABLE IF NOT EXISTS ads_simple (
            inserted_at DateTime DEFAULT now(),
            seq UInt64,
            campaign_id String,
            spend Float64,
            currency String
        ) ENGINE = MergeTree()
        ORDER BY seq
    """)

    print(f"[START] consuming from topic '{topic}' and writing to ClickHouse...")

    try:
        while True:
            msg = consumer.poll(1.0)  # poll for new messages, wait up to 1 second

            if msg is None:
                continue  # no message in this poll cycle

            if msg.error():
                raise KafkaException(msg.error())

            # Decode JSON message
            data = json.loads(msg.value().decode("utf-8"))

            seq = int(data["seq"])
            campaign_id = str(data["campaign_id"])
            spend = float(data["spend"])
            currency = str(data["currency"])

            print(f"[RECV] topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} value={data}")

            # Insert into ClickHouse
            client.insert(
                "ads_simple",
                [(seq, campaign_id, spend, currency)],
                column_names=["seq", "campaign_id", "spend", "currency"]
            )

    except KeyboardInterrupt:
        print("[STOP] stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

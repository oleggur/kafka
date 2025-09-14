#!/usr/bin/env python3
"""
Produces numbered JSON records to 'ads_retries' and prints delivery results.
Run once with idempotence OFF, once with idempotence ON, while bouncing the broker
mid-stream to trigger retries. Then compare duplicates by 'seq'.
"""

from __future__ import annotations
import argparse, json, time
from typing import Any, Dict, Optional
from confluent_kafka import Producer, KafkaError, Message

def build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Retry demo with optional idempotence.")
    ap.add_argument("--topic", type=str, default="ads_retries")
    ap.add_argument("--count", type=int, default=100)               # enough to span the outage window
    ap.add_argument("--sleep", type=float, default=0.03)            # small delay for readability
    ap.add_argument("--idempotent", action="store_true")            # toggle idempotence on
    return ap.parse_args()

def make_producer(idempotent: bool) -> Producer:
    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "acks": "all",                           # in 1-broker dev this ~= acks=1; keep habit for prod
        "enable.idempotence": idempotent,        # the star of this demo
        "delivery.timeout.ms": 120_000,          # retry budget
        "linger.ms": 5,                          # tiny micro-batching, not essential here
        "max.in.flight.requests.per.connection": 5,  # default; compatible with idempotence
    }
    return Producer(conf)

def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    key = msg.key().decode("utf-8") if msg.key() else None
    if err is not None:
        print(f"[DELIVERY-ERROR] key={key} err={err}")
    else:
        print(f"[DELIVERED] partition={msg.partition()} offset={msg.offset()} key={key}")

def main() -> None:
    args = build_args()
    p = make_producer(args.idempotent)
    print(f"[START] topic={args.topic} count={args.count} idempotent={args.idempotent}")
    for i in range(args.count):
        value = {"seq": i, "ts": time.time()}
        p.produce(args.topic, value=json.dumps(value).encode("utf-8"),
                  key=str(i).encode("utf-8"), on_delivery=delivery_report)
        p.poll(0)
        print(f"[SENT] seq={i}")
        time.sleep(args.sleep)
    leftover = p.flush(30)
    print(f"[FLUSHED] remaining={leftover}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, time
from typing import Any, Dict, Optional
from confluent_kafka import Consumer, KafkaException, KafkaError, Message

def build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Tail seq values from a topic and survive broker restarts.")
    ap.add_argument("--topic", type=str, default="ads_retries", help="Topic to read (default: ads_retries)")
    ap.add_argument("--group", type=str, default="g_retry_watch", help="Consumer group id (default: g_retry_watch)")
    ap.add_argument("--timeout", type=float, default=1.0, help="poll timeout seconds (default: 1.0)")
    return ap.parse_args()

def make_consumer(group_id: str) -> Consumer:
    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": group_id,
        "enable.auto.commit": True,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)

def process(msg: Message) -> None:
    raw: Optional[bytes] = msg.value()
    if not raw:
        print(f"[∅] {msg.topic()}[{msg.partition()}]@{msg.offset()}")
        return
    try:
        seq = json.loads(raw.decode("utf-8")).get("seq")
    except Exception:
        seq = "<non-json>"
    print(f"[RECV] {msg.topic()}[{msg.partition()}]@{msg.offset()} seq={seq}")

def main() -> None:
    args = build_args()
    c = make_consumer(args.group)
    c.subscribe([args.topic])
    print(f"[START] topic='{args.topic}', group='{args.group}' (Ctrl+C to stop)")
    try:
        while True:
            msg = c.poll(args.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # On broker down, you’ll see transient errors/timeouts; keep polling.
                print(f"[WARN] {msg.error()}")
                continue
            process(msg)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        print("[CLOSED] consumer closed.")

if __name__ == "__main__":
    main()

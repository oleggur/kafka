#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, time, uuid
from typing import Any, Dict, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

def build_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Read from beginning and report duplicate seq values.")
    ap.add_argument("--topic", type=str, default="ads_retries")
    ap.add_argument("--max", type=int, default=10000, help="Max messages to read (default: 10000)")
    ap.add_argument("--timeout", type=float, default=5.0, help="Idle seconds before stopping (default: 5.0)")
    return ap.parse_args()

def make_consumer() -> Consumer:
    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"g_check_{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)

def main() -> None:
    args = build_args()
    c = make_consumer()
    c.subscribe([args.topic])
    seen: Dict[int, int] = {}
    read = 0
    idle_start = time.time()
    try:
        while read < args.max and (time.time() - idle_start) < args.timeout:
            msg: Optional[Message] = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle_start = time.time()  # restart idle timer at each EOF
                    continue
                raise KafkaException(msg.error())
            idle_start = time.time()
            read += 1
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                seq = int(payload.get("seq"))
            except Exception:
                continue
            seen[seq] = seen.get(seq, 0) + 1
    finally:
        c.close()

    dups = {k: v for k, v in seen.items() if v > 1}
    print(f"[STATS] read={read} unique_seqs={len(seen)} duplicates={len(dups)}")
    if dups:
        print("[DUPLICATES]")
        for k in sorted(dups):
            print(f"  seq={k} count={dups[k]}")
    else:
        print("[DUPLICATES] none")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, time
from typing import Any, Dict, Optional
from confluent_kafka import Producer

def main() -> None:
    ap = argparse.ArgumentParser(description="Seed input topic with JSON rows.")
    ap.add_argument("--topic", type=str, default="ads_input")
    ap.add_argument("--count", type=int, default=20)
    ap.add_argument("--sleep", type=float, default=0.05)
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": "localhost:9092", "linger.ms": 5})
    for i in range(args.count):
        value = {"seq": i, "ts": time.time(), "campaign_id": f"c{i%3}"}
        p.produce(args.topic, json.dumps(value).encode("utf-8"))
        p.poll(0)
        print(f"[SEEDED] {value}")
        time.sleep(args.sleep)
    p.flush()
    print("[DONE] seeded.")

if __name__ == "__main__":
    main()

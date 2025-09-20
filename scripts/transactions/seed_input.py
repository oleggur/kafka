#!/usr/bin/env python3
"""
Seed the input topic with simple JSON events.

- Topic: ads_input
- Payload: {"seq": <int>, "ts": <float>, "campaign_id": "c0|c1|c2"}

Why: Gives the pipeline something to read. Multiple runs will reuse seq=0..N-1.
"""

from __future__ import annotations
import argparse, json, time
from typing import Any, Dict
from confluent_kafka import Producer


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed input topic with JSON rows.")
    ap.add_argument("--topic", type=str, default="ads_input",
                    help="Destination topic to seed (default: ads_input).")
    ap.add_argument("--count", type=int, default=20,
                    help="How many events to produce (default: 20).")
    ap.add_argument("--sleep", type=float, default=0.05,
                    help="Seconds to sleep between events (default: 0.05).")
    args = ap.parse_args()

    # Minimal producer config for local dev; batching is small but fine here
    p: Producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 5,  # tiny micro-batch for efficiency; you can set 0 too
    })

    for i in range(args.count):
        value: Dict[str, Any] = {
            "seq": i,                          # small increasing id (resets every run)
            "ts": time.time(),                 # unix timestamp
            "campaign_id": f"c{i % 3}",        # c0, c1, c2 (3-way key distribution)
        }
        p.produce(args.topic, json.dumps(value).encode("utf-8"))
        p.poll(0)  # serve delivery callbacks internally (we don't print them here)
        print(f"[SEEDED] {value}")
        time.sleep(args.sleep)

    p.flush()
    print("[DONE] seeded.")


if __name__ == "__main__":
    main()

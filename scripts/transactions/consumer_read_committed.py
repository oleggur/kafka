#!/usr/bin/env python3
"""
Downstream reader of 'ads_output' that only shows **committed** transactional data.

- isolation.level = read_committed → hides records from aborted transactions.
- You may observe "missing offsets" (e.g., see @10 then @12) because offsets consumed
  by aborted writes are still counted, but their data is hidden from you.
"""

from __future__ import annotations
import argparse, json
from typing import Any, Dict, Optional
from confluent_kafka import Consumer, KafkaError, KafkaException, Message


def main() -> None:
    ap = argparse.ArgumentParser(description="Read committed records from ads_output.")
    ap.add_argument("--topic", type=str, default="ads_output",
                    help="Topic to read from (default: ads_output).")
    ap.add_argument("--group", type=str, default="g_tx_out",
                    help="Consumer group id (default: g_tx_out).")
    ap.add_argument("--timeout", type=float, default=1.0,
                    help="poll() timeout seconds (default: 1.0).")
    args = ap.parse_args()

    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        "enable.auto.commit": True,         # background offset commits are fine for this demo
        "auto.offset.reset": "earliest",    # start from the beginning on first run
        "isolation.level": "read_committed" # <— hide aborted transactional writes
    }
    c: Consumer = Consumer(conf)
    c.subscribe([args.topic])

    print(f"[START] topic='{args.topic}', group='{args.group}', isolation=read_committed")
    try:
        while True:
            msg: Optional[Message] = c.poll(args.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # not an error; it just means no new data for this partition now
                    continue
                raise KafkaException(msg.error())
            body: str = msg.value().decode("utf-8") if msg.value() else "∅"
            print(f"[OUT] {msg.topic()}[{msg.partition()}]@{msg.offset()} {body}")
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        print("[CLOSED] read_committed consumer closed.")


if __name__ == "__main__":
    main()

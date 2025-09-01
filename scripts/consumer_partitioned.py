#!/usr/bin/env python3
"""
Day 3 — Partition-aware consumer (fully typed + heavily commented)

What this script demonstrates:
- Joining a *consumer group* and watching **partition assignments / revocations** (rebalances).
- Printing each message with its **partition and offset** so you see how load is split.
- Clean shutdown + optional batch-size to show when you'd commit in real apps.

Run (from repo root, with your venv activated):
    python scripts/consumer_partitioned.py
    # optional flags:
    #   --topic ads_partitioned
    #   --group g3_demo
    #   --batch 10          (how many messages to process before a placeholder "commit")
    #   --timeout 1.0       (poll timeout seconds)

Tips:
- Start two copies of this script with the SAME --group (e.g., g3_demo) to see partitions split.
- Start another copy with a DIFFERENT --group (e.g., g3_debug) to see "fan-out" (both groups get all data).
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaException, KafkaError, Message, TopicPartition


def build_args() -> argparse.Namespace:
    """
    Parse CLI flags for the script.

    Returns:
        argparse.Namespace with:
            topic (str):   topic to consume
            group (str):   consumer group id (enables offset tracking & rebalancing)
            batch (int):   messages to process before placeholder "commit"
            timeout (float): poll timeout in seconds
    """
    ap = argparse.ArgumentParser(description="Partition-aware Kafka consumer with assignment/revocation logs.")
    ap.add_argument("--topic", type=str, default="ads_partitioned",
                    help="Topic name to consume (default: ads_partitioned).")
    ap.add_argument("--group", type=str, default="g3_demo",
                    help="Consumer group id (default: g3_demo). Use same id in multiple processes to split work.")
    ap.add_argument("--batch", type=int, default=10,
                    help="How many messages to process before a placeholder commit log (default: 10).")
    ap.add_argument("--timeout", type=float, default=1.0,
                    help="poll() timeout in seconds (default: 1.0).")
    return ap.parse_args()


def _fmt_tps(tps: List[TopicPartition]) -> str:
    """Pretty-print a list of TopicPartition objects."""
    return ", ".join(f"{tp.topic}[{tp.partition}]" for tp in tps)


def on_assign(consumer: Consumer, partitions: List[TopicPartition]) -> None:
    """
    Called when this consumer instance is assigned partitions after a rebalance.
    Great place to log *which* partitions you’re responsible for.
    """
    print(f"[ASSIGN] now responsible for: {_fmt_tps(partitions)}")
    # Optional: seek to a specific offset per partition here if you want custom positioning.
    consumer.assign(partitions)  # Accept the assignment as-is.


def on_revoke(consumer: Consumer, partitions: List[TopicPartition]) -> None:
    """
    Called when a rebalance revokes partitions from this consumer (e.g., another consumer joined/left).
    """
    print(f"[REVOKE] giving up: {_fmt_tps(partitions)}")


def make_consumer(group_id: str) -> Consumer:
    """
    Build a Kafka Consumer with sane dev defaults.

    Notes:
    - enable.auto.commit=True lets the library periodically commit offsets for you (simple for demos).
      For precise control, set it to False and commit after your durable side effects (Day 2 manual commit).
    - auto.offset.reset='earliest' lets new groups replay from the beginning.
    """
    conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",  # host listener exposed by docker-compose
        "group.id": group_id,                   # consumers with the same group id share the work
        "enable.auto.commit": True,             # simple demo; commit every interval in background
        "auto.offset.reset": "earliest",        # if no committed offsets exist, start from the head of the log
        # Tuning knobs you might touch later:
        # "session.timeout.ms": 10000,
        # "max.poll.interval.ms": 300000,
    }
    return Consumer(conf)


# Global stop flag toggled by signal handlers
_SHOULD_STOP: bool = False


def _install_signal_handlers() -> None:
    """Allow Ctrl+C / SIGTERM to break the loop gracefully."""
    def _sig_handler(sig: int, _frame: Any) -> None:
        global _SHOULD_STOP
        print("\n[SHUTDOWN] signal received, closing…")
        _SHOULD_STOP = True

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)


def process(msg: Message) -> None:
    """
    Example "business logic":
    - Decode the message (we treat values as UTF-8 JSON if possible; otherwise print raw).
    - Print partition + offset so distribution is obvious.

    Real apps would write to a DB (idempotently), call APIs, etc.
    """
    part: int = msg.partition()
    off: int = msg.offset()
    key: Optional[str] = msg.key().decode("utf-8") if msg.key() else None
    raw: Optional[bytes] = msg.value()

    # Try to pretty-print JSON if the payload looks like JSON; fall back to raw.
    if raw is None:
        body: str = "∅"
    else:
        try:
            body = json.dumps(json.loads(raw.decode("utf-8")), ensure_ascii=False)
        except Exception:
            body = raw.decode("utf-8", errors="replace")

    print(f"[RECV] {msg.topic()}[{part}]@{off} key={key} value={body}")


def main() -> None:
    args = build_args()
    _install_signal_handlers()

    consumer: Consumer = make_consumer(args.group)

    # Subscribe with assignment callbacks so we SEE rebalances as they happen.
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    print(f"[START] topic='{args.topic}', group='{args.group}', auto_commit=True, reset=earliest")
    print("        Start another instance of this script with the SAME --group to see a rebalance.\n")

    processed_in_batch: int = 0

    try:
        while not _SHOULD_STOP:
            # poll() asks broker for a message, waiting up to args.timeout seconds.
            msg: Optional[Message] = consumer.poll(args.timeout)

            if msg is None:
                # No message in this interval; in real apps you might flush metrics here.
                continue

            if msg.error():
                # _PARTITION_EOF is just “we read to the end for now” — not fatal.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                # Otherwise raise so you see the problem.
                raise KafkaException(msg.error())

            # ----- Process one message -----
            process(msg)
            processed_in_batch += 1

            # Placeholder place for batch commits if you disable auto-commit:
            if processed_in_batch >= args.batch:
                # With enable.auto.commit=True this is just a log line.
                # If you had manual commit, you'd call consumer.commit() here (see Day 2 manual example).
                print(f"[BATCH] processed {processed_in_batch} messages (auto-commit handles offsets).")
                processed_in_batch = 0

    except KeyboardInterrupt:
        # Redundant thanks to _install_signal_handlers, but safe.
        pass
    finally:
        # Always close so the consumer leaves the group cleanly (and final auto-commit occurs if enabled).
        consumer.close()
        print("[CLOSED] consumer closed cleanly.")


if __name__ == "__main__":
    main()

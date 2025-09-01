#!/usr/bin/env python3
"""
Day 2 — JSON Consumer with **manual commits** (typed, super-commented)

Goal:
- Show how to control *when* offsets are committed (after processing succeeds).
- Read from topic 'ads_raw_json' and process in small batches.
- Demonstrate both sync and async commit styles.

Key ideas:
- "Processing" = your business logic (e.g., write to ClickHouse).
- We commit ONLY after processing the batch successfully (classic at-least-once).
- If we crash before commit, the batch will be re-read on restart (duplicates possible → make processing idempotent).

Run:
    python scripts/consumer_json_manual.py
"""


from __future__ import annotations

import json
import signal
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, KafkaException, KafkaError, Message, TopicPartition

# ----------------------------
# Config knobs (tweak freely)
# ----------------------------
TOPIC: str = "ads_raw_json"      # source topic (from Day 2 Phase A producer)
GROUP_ID: str = "g_json_manual"  # a new group so we don't reuse auto-commit offsets
BATCH_SIZE: int = 5              # how many messages to process before committing
POLL_TIMEOUT_S: float = 1.0      # poll() timeout (seconds)
USE_ASYNC_COMMIT: bool = False   # False = synchronous commit; True = asynchronous commit

# ----------------------------
# Consumer configuration
# ----------------------------
# - enable.auto.commit = False → we control commits explicitly
# - auto.offset.reset = 'earliest' → start at beginning if no committed offsets exist for this group
# - max.poll.interval.ms controls max processing time between polls before the broker rebalances you out
consumer_conf: Dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    "group.id": GROUP_ID,
    "enable.auto.commit": False,          # MANUAL COMMITS
    "auto.offset.reset": "earliest",
    "max.poll.interval.ms": 300_000,      # 5 minutes
}

consumer: Consumer = Consumer(consumer_conf)

# Global flag for graceful shutdown via Ctrl+C
_SHOULD_STOP: bool = False

def _signal_handler(sig: int, frame: Optional[Any]) -> None:
    """Handle SIGINT/SIGTERM so we can flush/commit cleanly before exit."""
    global _SHOULD_STOP
    print("\n[SHUTDOWN] signal received, finishing current batch then exiting…")
    _SHOULD_STOP = True

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def process_record(msg: Message) -> None:
    """
    Your business logic goes here.

    NOTE on idempotency:
    - Make external side effects idempotent (e.g., DB upsert by unique key),
      because with at-least-once, a crash can cause reprocessing.
    """
    raw: Optional[bytes] = msg.value()
    if raw is None:
        print(f"[SKIP] empty value at {msg.topic()}[{msg.partition()}]@{msg.offset()}")
        return

    value: Dict[str, Any] = json.loads(raw.decode("utf-8"))
    key: Optional[str] = msg.key().decode("utf-8") if msg.key() else None

    # Simulate work here (e.g., write to ClickHouse)
    print(
        f"[PROCESS] {msg.topic()}[{msg.partition()}]@{msg.offset()} "
        f"key={key} value={value}"
    )

def _build_commit_offsets(batch: List[Message]) -> List[TopicPartition]:
    """
    Build a list of TopicPartition objects at the *next* offset to read per partition.
    - If we processed offsets [10,11,12] in partition P, we must commit 13.
    """
    if not batch:
        return []

    # Track the highest seen offset per (topic, partition)
    max_by_tp: Dict[Tuple[str, int], int] = {}
    for m in batch:
        tp: Tuple[str, int] = (m.topic(), m.partition())
        current_max: int = max_by_tp.get(tp, -1)
        if m.offset() > current_max:
            max_by_tp[tp] = m.offset()

    # Build TopicPartition list with offset+1 (the next record to read)
    commits: List[TopicPartition] = []
    for (topic, partition), max_off in max_by_tp.items():
        commits.append(TopicPartition(topic=topic, partition=partition, offset=max_off + 1))
    return commits

def commit_offsets(batch: List[Message]) -> None:
    """
    Commit offsets for all partitions touched in the batch.
    - Synchronous commit: blocks until broker acks (clearer failure semantics).
    - Asynchronous commit: fire-and-forget; handle callback for errors.
    """
    commits: List[TopicPartition] = _build_commit_offsets(batch)
    if not commits:
        return

    try:
        if USE_ASYNC_COMMIT:
            def _on_commit(err: KafkaError, parts: List[TopicPartition]) -> None:
                if err is not None:
                    print(f"[COMMIT-ASYNC-ERROR] {err} on {[(p.topic, p.partition, p.offset) for p in parts]}")
                else:
                    print(f"[COMMIT-ASYNC-OK] {[(p.topic, p.partition, p.offset) for p in parts]}")
            consumer.commit(offsets=commits, asynchronous=True, callback=_on_commit)
        else:
            parts: List[TopicPartition] = consumer.commit(offsets=commits, asynchronous=False)
            print(f"[COMMIT-SYNC-OK] {[(p.topic, p.partition, p.offset) for p in parts]}")
    except KafkaException as e:
        print(f"[COMMIT-ERROR] {e}")
        # Decide retry policy; commits are idempotent so retry is usually safe.

def main() -> None:
    consumer.subscribe([TOPIC])
    print(
        f"[START] topic='{TOPIC}', group='{GROUP_ID}', batch_size={BATCH_SIZE}, "
        f"manual_commits=True, async={USE_ASYNC_COMMIT}"
    )

    batch: List[Message] = []

    try:
        while not _SHOULD_STOP:
            msg: Optional[Message] = consumer.poll(POLL_TIMEOUT_S)

            if msg is None:
                if batch:
                    print(f"[BATCH] timeout with partial batch size={len(batch)} → committing")
                    commit_offsets(batch)
                    batch.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if batch:
                        print(f"[BATCH] EOF with partial batch size={len(batch)} → committing")
                        commit_offsets(batch)
                        batch.clear()
                    continue
                raise KafkaException(msg.error())

            # ----- PROCESS -----
            process_record(msg)
            batch.append(msg)

            if len(batch) >= BATCH_SIZE:
                commit_offsets(batch)
                batch.clear()

        # Graceful stop → commit any remainder
        if batch:
            print(f"[BATCH] shutdown with partial batch size={len(batch)} → committing")
            commit_offsets(batch)
            batch.clear()

    finally:
        consumer.close()
        print("[CLOSED] consumer closed cleanly.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Transactional pipeline: read from 'ads_input' (group 'g_tx_in'),
process, write to 'ads_output', and commit input offsets within the same transaction.

Exactly-once across the A→B hop for read_committed readers.
"""

from __future__ import annotations
import argparse, json, sys
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import KafkaException, KafkaError, Message, TopicPartition
from confluent_kafka import Consumer, Producer

# ---------- Small helpers ----------

def _process(value: Dict[str, Any]) -> Dict[str, Any]:
    """Toy transform: add spend bucket and copy campaign_id."""
    spend = float(value.get("seq", 0)) * 1.11
    bucket = "low" if spend < 10 else "mid" if spend < 30 else "high"
    return {"seq": value.get("seq"), "campaign_id": value.get("campaign_id"), "spend": spend, "bucket": bucket}

def _json(msg: Message) -> Optional[Dict[str, Any]]:
    if msg.value() is None:
        return None
    try:
        return json.loads(msg.value().decode("utf-8"))
    except Exception:
        return None

# ---------- Main ----------

def main() -> None:
    ap = argparse.ArgumentParser(description="Transactional read→process→write pipeline A→B.")
    ap.add_argument("--in-topic", type=str, default="ads_input")
    ap.add_argument("--out-topic", type=str, default="ads_output")
    ap.add_argument("--group", type=str, default="g_tx_in")
    ap.add_argument("--transactional-id", type=str, default="tx-pipeline-1")
    ap.add_argument("--batch", type=int, default=10, help="records per transaction")
    ap.add_argument("--abort-after", type=int, default=-1, help="if >0, abort transaction after N records (demo)")
    args = ap.parse_args()

    # Consumer for input A
    consumer_conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        "enable.auto.commit": False,       # we will commit offsets via the transaction
        "auto.offset.reset": "earliest",
    }
    c: Consumer = Consumer(consumer_conf)
    c.subscribe([args.in_topic])

    # Transactional producer for output B
    prod_conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "transactional.id": args.transactional_id,  # enables idempotence + transactions
        "acks": "all",
        # Timeouts left default; production apps tune these
    }
    p: Producer = Producer(prod_conf)

    print(f"[INIT] initializing transactions for transactional.id={args.transactional_id} ...")
    p.init_transactions()   # may contact coordinator; do once per process
    print("[INIT] ready.")

    processed_total: int = 0

    try:
        while True:
            # --- Begin a new transaction for a batch ---
            p.begin_transaction()
            batch_msgs: List[Message] = []
            print(f"\n[TX] begin (target batch={args.batch})")

            # Poll loop to build a batch
            while len(batch_msgs) < args.batch:
                msg: Optional[Message] = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # No new data for now; if we have some in batch, break to commit/abort
                        if batch_msgs:
                            break
                        else:
                            continue
                    raise KafkaException(msg.error())
                batch_msgs.append(msg)

                # Optional demo: force an abort after N records
                if args.abort_after > 0 and processed_total + len(batch_msgs) >= args.abort_after:
                    print(f"[TX] abort demo after {args.abort_after} records total")
                    p.abort_transaction()
                    # Do NOT commit offsets; data is invisible to read_committed; offsets unchanged
                    batch_msgs.clear()
                    # Start a fresh transaction
                    p.begin_transaction()

            if not batch_msgs:
                # nothing to do; continue polling
                continue

            # --- Produce transformed outputs to B ---
            for m in batch_msgs:
                val_in: Optional[Dict[str, Any]] = _json(m)
                if val_in is None:
                    continue
                out: Dict[str, Any] = _process(val_in)
                key_bytes = (str(out.get("campaign_id"))).encode("utf-8")
                p.produce(args.out_topic, key=key_bytes, value=json.dumps(out).encode("utf-8"))

            # Pump callbacks (optional)
            p.poll(0)

            # --- Atomically attach input offsets to this transaction ---
            # Build the offsets "next position to read" per partition
            offsets: List[TopicPartition] = []
            max_by_tp: Dict[Tuple[str, int], int] = {}
            for m in batch_msgs:
                tp = (m.topic(), m.partition())
                cur = max_by_tp.get(tp, -1)
                if m.offset() > cur:
                    max_by_tp[tp] = m.offset()
            for (topic, part), max_off in max_by_tp.items():
                offsets.append(TopicPartition(topic, part, max_off + 1))

            # Attach the committing of these offsets (for group args.group) to the transaction
            group_md = c.consumer_group_metadata()
            p.send_offsets_to_transaction(offsets, group_md)

            # --- Commit the transaction ---
            p.commit_transaction()
            processed_total += len(batch_msgs)
            print(f"[TX] commit ok (batch={len(batch_msgs)}) total={processed_total}")

    except KeyboardInterrupt:
        print("\n[STOP] KeyboardInterrupt")
    finally:
        c.close()
        try:
            # If a transaction is in-flight and we’re exiting, abort to be safe
            p.abort_transaction()
        except Exception:
            pass
        print("[CLOSED] pipeline closed.")

if __name__ == "__main__":
    main()

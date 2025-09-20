#!/usr/bin/env python3
"""
Transactional pipeline: read from 'ads_input', transform, write to 'ads_output',
and commit input offsets *within the same transaction*.

Exactly-once across the A→B hop for downstream consumers using isolation.level=read_committed.

Flow per batch:
  p.begin_transaction()
    - poll input messages into a batch
    - produce transformed outputs to ads_output
    - send_offsets_to_transaction(...)  # attach input offsets (group g_tx_in) to this tx
  p.commit_transaction()  # outputs + offsets become visible atomically
(or p.abort_transaction() on errors → outputs invisible, offsets not advanced)

Notes:
- Transactions require a stable 'transactional.id' per logical app instance.
- Keep transactions reasonably small/short to avoid long uncommitted tails.
"""

from __future__ import annotations
import argparse, json
from typing import Any, Dict, List, Optional, Tuple

from confluent_kafka import Consumer, Producer
from confluent_kafka import KafkaException, KafkaError, Message, TopicPartition


# -------- Helpers --------

def _process(value: Dict[str, Any]) -> Dict[str, Any]:
    """
    Toy transform: compute a synthetic 'spend' from seq (seq * 1.11) and bucket it.
    This simulates some business logic you'd normally run between A and B.
    """
    seq: int = int(value.get("seq", 0))
    spend: float = seq * 1.11
    bucket: str = "low" if spend < 10 else ("mid" if spend < 30 else "high")
    return {
        "seq": seq,
        "campaign_id": value.get("campaign_id"),
        "spend": spend,
        "bucket": bucket,
    }


def _json(msg: Message) -> Optional[Dict[str, Any]]:
    """Safely decode JSON from a Kafka message; return None on decode errors or tombstones."""
    if msg.value() is None:
        return None
    try:
        return json.loads(msg.value().decode("utf-8"))
    except Exception:
        return None


# -------- Main --------

def main() -> None:
    ap = argparse.ArgumentParser(description="Transactional read→process→write pipeline A→B.")
    ap.add_argument("--in-topic", type=str, default="ads_input",
                    help="Source topic to consume (default: ads_input).")
    ap.add_argument("--out-topic", type=str, default="ads_output",
                    help="Destination topic to produce (default: ads_output).")
    ap.add_argument("--group", type=str, default="g_tx_in",
                    help="Consumer group id for input (default: g_tx_in).")
    ap.add_argument("--transactional-id", type=str, default="tx-pipeline-1",
                    help="Stable transactional id for this pipeline instance (default: tx-pipeline-1).")
    ap.add_argument("--batch", type=int, default=10,
                    help="How many records per transaction (default: 10).")
    ap.add_argument("--abort-after", type=int, default=-1,
                    help="If >0, abort once total processed reaches N (demo aborted tx).")
    args = ap.parse_args()

    # 1) Build input consumer (manual commit disabled; offsets will be committed inside the tx)
    consumer_conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": args.group,
        "enable.auto.commit": False,      # commit via transaction to bind offsets with outputs
        "auto.offset.reset": "earliest",  # start from beginning if no offsets exist
    }
    c: Consumer = Consumer(consumer_conf)
    c.subscribe([args.in_topic])

    # 2) Build transactional producer for output
    #    Setting transactional.id automatically enables idempotence under the hood.
    prod_conf: Dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "transactional.id": args.transactional_id,
        "acks": "all",
    }
    p: Producer = Producer(prod_conf)

    # 3) Initialize transactions (contact coordinator, get PID/epoch, fencing protection)
    print(f"[INIT] initializing transactions for transactional.id={args.transactional_id} ...")
    p.init_transactions()
    print("[INIT] ready.")

    processed_total: int = 0

    try:
        while True:
            # --- Begin a new transaction (one per small batch) ---
            p.begin_transaction()
            batch_msgs: List[Message] = []
            print(f"\n[TX] begin (target batch={args.batch})")

            # --- Collect up to 'batch' input messages ---
            while len(batch_msgs) < args.batch:
                msg: Optional[Message] = c.poll(1.0)  # wait up to 1s for data
                if msg is None:
                    continue

                if msg.error():
                    # _PARTITION_EOF means we reached end of a partition for now – not fatal.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        if batch_msgs:
                            break  # commit what we have in the batch
                        else:
                            continue
                    # Other errors are raised to stop the app / surface the issue.
                    raise KafkaException(msg.error())

                batch_msgs.append(msg)

                # Optional demo: force an abort once we've processed N records overall
                if args.abort_after > 0 and processed_total + len(batch_msgs) >= args.abort_after:
                    print(f"[TX] abort demo after {args.abort_after} records total")
                    p.abort_transaction()   # outputs invisible to read_committed; offsets not advanced
                    batch_msgs.clear()
                    p.begin_transaction()   # start a fresh transaction for continued processing

            if not batch_msgs:
                # No data arrived; abort this empty tx and continue polling
                p.abort_transaction()
                continue

            # --- Produce transformed outputs to the destination topic ---
            for m in batch_msgs:
                vin: Optional[Dict[str, Any]] = _json(m)
                if vin is None:
                    continue
                vout: Dict[str, Any] = _process(vin)

                # Use campaign_id as key → stable partition per campaign, preserves per-key order.
                key_bytes: bytes = str(vout.get("campaign_id")).encode("utf-8")
                p.produce(args.out_topic, key=key_bytes, value=json.dumps(vout).encode("utf-8"))

            p.poll(0)  # drive callbacks; not strictly required here

            # --- Build the offset list "next position to read" per (topic, partition) ---
            max_by_tp: Dict[Tuple[str, int], int] = {}
            for m in batch_msgs:
                tp = (m.topic(), m.partition())
                cur_max: int = max_by_tp.get(tp, -1)
                if m.offset() > cur_max:
                    max_by_tp[tp] = m.offset()

            offsets: List[TopicPartition] = [
                TopicPartition(topic, part, max_off + 1) for (topic, part), max_off in max_by_tp.items()
            ]

            # --- Atomically attach input offsets for this consumer group to this transaction ---
            group_md = c.consumer_group_metadata()
            p.send_offsets_to_transaction(offsets, group_md)  # positional args in Python API

            # --- Commit the transaction: outputs + offsets become visible together ---
            p.commit_transaction()
            processed_total += len(batch_msgs)
            print(f"[TX] commit ok (batch={len(batch_msgs)}) total={processed_total}")

    except KeyboardInterrupt:
        print("\n[STOP] KeyboardInterrupt")
    finally:
        # Close consumer and ensure any open transaction is aborted on shutdown
        c.close()
        try:
            p.abort_transaction()
        except Exception:
            pass
        print("[CLOSED] pipeline closed.")


if __name__ == "__main__":
    main()

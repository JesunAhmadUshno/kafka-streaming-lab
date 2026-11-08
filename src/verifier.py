"""Verifier: reads the whole output topic with a fresh group and
counts duplicates, losses, and out-of-order arrivals from the
sequence numbers alone (PREREGISTRATION.md Sections 2 and 8).
"""

import argparse
import json
import sys
import time

from confluent_kafka import Consumer, KafkaError

sys.path.insert(0, "src")
from common import SENTINEL_SEQ, parse_output_value, write_json_atomic
from counting import count_correctness


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--ack-log", required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    acked = set()
    with open(args.ack_log) as fh:
        for line in fh:
            rec = json.loads(line)
            if rec["err"] is None and rec["seq"] is not None and rec["seq"] >= 0:
                acked.add(rec["seq"])

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.topic])
    records = []
    sentinel_seen_at = None
    last_msg_at = time.time()
    deadline = time.time() + 240
    while time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is None:
            now = time.time()
            if sentinel_seen_at is not None and now - last_msg_at > 3.0:
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                now = time.time()
                if sentinel_seen_at is not None and now - last_msg_at > 3.0:
                    break
                continue
            if msg.error().fatal():
                raise SystemExit(f"verifier fatal consumer error: {msg.error()}")
            continue
        seq, t_in, t_out = parse_output_value(msg.value())
        last_msg_at = time.time()
        if seq == SENTINEL_SEQ:
            if sentinel_seen_at is None:
                sentinel_seen_at = time.time()
            continue
        records.append((seq, t_in, t_out))
    consumer.close()

    result = count_correctness(records, acked)
    result["acked_total"] = len(acked)
    result["sentinel_seen"] = sentinel_seen_at is not None
    result["expected_n"] = args.n
    write_json_atomic(args.out, result)
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())

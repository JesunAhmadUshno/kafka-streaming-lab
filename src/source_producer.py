"""Source producer: writes N sequenced messages plus one sentinel to
the input topic as fast as the client allows, and logs every delivery
outcome. Settings per PREREGISTRATION.md Section 4.
"""

import argparse
import json
import sys
import time

from confluent_kafka import Producer

sys.path.insert(0, "src")
from common import SENTINEL_SEQ, make_value, now_ms, write_json_atomic


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--config", choices=["A", "B"], required=True)
    ap.add_argument("--ack-log", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        "acks": "all",
        "retries": 10,
        "message.timeout.ms": 120000,
        "queue.buffering.max.messages": 200000,
    }
    if args.config == "A":
        conf["enable.idempotence"] = False
    else:
        conf["enable.idempotence"] = True

    state = {"acked": 0, "errors": 0}
    ack_fh = open(args.ack_log, "w", buffering=1)

    def on_delivery(err, msg):
        key = msg.key()
        seq = int(key.decode()) if key is not None else None
        rec = {
            "seq": seq,
            "t_ack": now_ms(),
            "err": str(err) if err is not None else None,
        }
        if err is None:
            state["acked"] += 1
        else:
            state["errors"] += 1
        ack_fh.write(json.dumps(rec) + "\n")

    producer = Producer(conf)
    t_first_send = None
    sent = 0
    for seq in list(range(args.n)) + [SENTINEL_SEQ]:
        t_send = now_ms()
        if t_first_send is None:
            t_first_send = t_send
        value = make_value(seq, t_send)
        while True:
            try:
                producer.produce(
                    args.topic,
                    key=str(seq).encode(),
                    value=value,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                producer.poll(0.5)
        sent += 1
        producer.poll(0)
        if sent % 500 == 0:
            write_json_atomic(
                args.progress,
                {"sent": sent, "acked": state["acked"], "t": now_ms()},
            )
    remaining = producer.flush(120)
    t_last = now_ms()
    write_json_atomic(args.progress, {"sent": sent, "acked": state["acked"], "t": t_last})
    summary = {
        "role": "source",
        "sent_total_incl_sentinel": sent,
        "acked_callbacks_ok": state["acked"],
        "delivery_errors": state["errors"],
        "unflushed_after_flush": remaining,
        "t_first_send": t_first_send,
        "t_end": t_last,
    }
    write_json_atomic(args.summary, summary)
    ack_fh.close()
    print(json.dumps(summary))
    return 0 if remaining == 0 else 4


if __name__ == "__main__":
    sys.exit(main())

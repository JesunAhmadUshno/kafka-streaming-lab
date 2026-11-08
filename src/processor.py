"""Processor: consumes the input topic, writes one output record per
input message, finishes at the sentinel. Two modes per
PREREGISTRATION.md Section 4:

A: auto-committing consumer, plain asynchronous producer, flush at the
   sentinel.
B: read-committed consumer with manual offsets, transactional producer
   committing output writes and consumer offsets together in batches
   of at most 100.
"""

import argparse
import json
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition

sys.path.insert(0, "src")
from common import SENTINEL_SEQ, make_output_value, now_ms, parse_value, write_json_atomic

BATCH = 100


def out_value(raw):
    seq, t_send = parse_value(raw)
    return seq, make_output_value(seq, t_send, now_ms())


class Progress:
    def __init__(self, path):
        self.path = path
        self.consumed = 0
        self.produced = 0

    def bump(self, force=False):
        if force or self.consumed % 100 == 0:
            write_json_atomic(
                self.path,
                {"consumed": self.consumed, "produced": self.produced, "t": now_ms()},
            )


def run_a(args):
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 1000,
            "enable.auto.offset.store": True,
            "isolation.level": "read_uncommitted",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "retries": 10,
            "enable.idempotence": False,
            "message.timeout.ms": 120000,
            "queue.buffering.max.messages": 200000,
        }
    )
    consumer.subscribe([args.input_topic])
    prog = Progress(args.progress)
    done = False
    while not done:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            if msg.error().fatal():
                raise KafkaException(msg.error())
            continue  # transient (for example the broker is down mid-fault)
        seq, value = out_value(msg.value())
        while True:
            try:
                producer.produce(args.output_topic, key=str(seq).encode(), value=value)
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)
        if seq == SENTINEL_SEQ:
            producer.flush(120)
            prog.produced += 1
            prog.bump(force=True)
            done = True
        else:
            prog.consumed += 1
            prog.produced += 1
            prog.bump()
    consumer.close()
    return prog


def run_b(args):
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "transactional.id": args.txn_id,
            "transaction.timeout.ms": 120000,
        }
    )
    producer.init_transactions(120)
    consumer.subscribe([args.input_topic])
    prog = Progress(args.progress)
    done = False
    while not done:
        # Collect one batch.
        batch = []
        while len(batch) < BATCH:
            msg = consumer.poll(0.25 if batch else 0.5)
            if msg is None:
                if batch:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if batch:
                        break
                    continue
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                continue  # transient; keep collecting
            batch.append(msg)
            if parse_value(msg.value())[0] == SENTINEL_SEQ:
                break
        if not batch:
            continue
        # Commit the batch transactionally; on a retriable failure,
        # abort and re-consume the same batch from its start offsets.
        while True:
            try:
                producer.begin_transaction()
                has_sentinel = False
                for msg in batch:
                    seq, value = out_value(msg.value())
                    producer.produce(args.output_topic, key=str(seq).encode(), value=value)
                    if seq == SENTINEL_SEQ:
                        has_sentinel = True
                positions = consumer.position(consumer.assignment())
                producer.send_offsets_to_transaction(
                    positions, consumer.consumer_group_metadata(), 60
                )
                producer.commit_transaction(60)
                break
            except KafkaException as exc:
                err = exc.args[0]
                try:
                    producer.abort_transaction(60)
                except KafkaException:
                    pass
                if err.retriable() or err.code() in (
                    KafkaError._TIMED_OUT,
                    KafkaError._TRANSPORT,
                    KafkaError._ALL_BROKERS_DOWN,
                    KafkaError._OUTDATED,
                ):
                    starts = {}
                    for msg in batch:
                        starts.setdefault(msg.topic(), {})
                        key = msg.partition()
                        cur = starts[msg.topic()].get(key)
                        if cur is None or msg.offset() < cur:
                            starts[msg.topic()][key] = msg.offset()
                    for topic, parts in starts.items():
                        for part, off in parts.items():
                            consumer.seek(TopicPartition(topic, part, off))
                    batch = []
                    has_sentinel = False
                    # Re-collect the same batch after the seek.
                    while len(batch) < BATCH:
                        msg = consumer.poll(0.5)
                        if msg is None:
                            if batch:
                                break
                            continue
                        if msg.error():
                            if msg.error().code() == KafkaError._PARTITION_EOF:
                                if batch:
                                    break
                                continue
                            if msg.error().fatal():
                                raise KafkaException(msg.error())
                            continue  # transient; keep collecting
                        batch.append(msg)
                        if parse_value(msg.value())[0] == SENTINEL_SEQ:
                            break
                    continue
                raise
        prog.consumed += sum(1 for m in batch if parse_value(m.value())[0] != SENTINEL_SEQ)
        prog.produced += len(batch)
        prog.bump(force=True)
        if has_sentinel:
            done = True
    consumer.close()
    return prog


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--input-topic", required=True)
    ap.add_argument("--output-topic", required=True)
    ap.add_argument("--group", required=True)
    ap.add_argument("--config", choices=["A", "B"], required=True)
    ap.add_argument("--txn-id", default="")
    ap.add_argument("--progress", required=True)
    ap.add_argument("--summary", required=True)
    args = ap.parse_args()

    t_start = now_ms()
    try:
        prog = run_a(args) if args.config == "A" else run_b(args)
        status = "completed"
        exit_code = 0
    except KafkaException as exc:
        err = exc.args[0] if exc.args else None
        summary = {
            "role": "processor",
            "status": "kafka_error",
            "error": str(exc),
            "error_code": str(err.code()) if err is not None else None,
            "fatal": bool(err.fatal()) if err is not None and hasattr(err, "fatal") else None,
            "t_start": t_start,
            "t_end": now_ms(),
        }
        write_json_atomic(args.summary, summary)
        print(json.dumps(summary))
        return 3
    summary = {
        "role": "processor",
        "status": status,
        "consumed_data": prog.consumed,
        "produced_incl_sentinel": prog.produced,
        "t_start": t_start,
        "t_end": now_ms(),
    }
    write_json_atomic(args.summary, summary)
    print(json.dumps(summary))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

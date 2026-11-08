"""Shared helpers for the Kafka streaming lab harness.

Message format (frozen in PREREGISTRATION.md Section 3): every value is
exactly 256 bytes. It is a JSON header carrying the sequence number and
the producer send timestamp in milliseconds, padded with a filler
character to 256 bytes. The sentinel has seq = -1.
"""

import json
import os
import time

VALUE_BYTES = 256
SENTINEL_SEQ = -1


def now_ms():
    return int(time.time() * 1000)


def make_value(seq, t_send_ms):
    header = json.dumps({"s": seq, "t": t_send_ms}, separators=(",", ":"))
    raw = header.encode("utf-8")
    if len(raw) > VALUE_BYTES:
        raise ValueError("header exceeds value size")
    return raw + b"x" * (VALUE_BYTES - len(raw))


def parse_value(raw):
    head = raw.decode("utf-8").rstrip("x")
    obj = json.loads(head)
    return obj["s"], obj["t"]


def make_output_value(seq, t_send_ms, t_out_ms):
    header = json.dumps(
        {"s": seq, "tin": t_send_ms, "tout": t_out_ms}, separators=(",", ":")
    )
    raw = header.encode("utf-8")
    if len(raw) > VALUE_BYTES:
        raise ValueError("header exceeds value size")
    return raw + b"y" * (VALUE_BYTES - len(raw))


def parse_output_value(raw):
    head = raw.decode("utf-8").rstrip("y")
    obj = json.loads(head)
    return obj["s"], obj["tin"], obj["tout"]


def write_json_atomic(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(obj, fh)
    os.replace(tmp, path)

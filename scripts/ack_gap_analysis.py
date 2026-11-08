"""Acknowledgement-gap analysis for broker_kill runs.

Reads the raw source acknowledgement logs (.work/runs/<runid>/
ack_log.jsonl) on the machine of record and writes
results/ack_gaps.json with, per broker_kill run, the longest gap
between consecutive successful acknowledgements. In a broker_kill
run that gap is the outage silence: the interval during which the
producer received no acknowledgement at all.

This replaces the driver's first-pass ack_gap_ms, which measured
across the driver's trigger timestamp (recorded before docker kill
actually stopped the broker) and so produced artifacts; see
AUDIT.md. Run on the machine of record after the matrix:

    python3 scripts/ack_gap_analysis.py
"""

import glob
import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    out = {}
    for path in sorted(glob.glob(os.path.join(REPO, "results", "runs", "*broker_kill*.json"))):
        with open(path) as fh:
            rec = json.load(fh)
        runid = rec["runid"]
        log_path = os.path.join(REPO, ".work", "runs", runid, "ack_log.jsonl")
        if not os.path.exists(log_path):
            print(f"{runid}: ack log not found at {log_path}, skipped")
            continue
        acks = []
        with open(log_path) as fh:
            for line in fh:
                entry = json.loads(line)
                if entry["err"] is None:
                    acks.append(entry["t_ack"])
        acks.sort()
        max_gap = 0
        for prev, cur in zip(acks, acks[1:]):
            max_gap = max(max_gap, cur - prev)
        out[runid] = {"max_ack_gap_ms": max_gap, "acks_ok": len(acks)}
        print(f"{runid}: acks_ok={len(acks)} max_ack_gap_ms={max_gap}")
    dest = os.path.join(REPO, "results", "ack_gaps.json")
    with open(dest, "w") as fh:
        json.dump(out, fh, indent=1, sort_keys=True)
    print(f"wrote {dest} with {len(out)} runs")
    return 0


if __name__ == "__main__":
    sys.exit(main())

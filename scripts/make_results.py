"""Generate RESULTS.md and results/summary.json from the run records
in results/runs/, under the frozen definitions of
PREREGISTRATION.md Sections 8 and 9. Nothing is hand-typed.
"""

import glob
import json
import os
import statistics
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCENARIOS = ["baseline", "consumer_kill", "broker_kill", "rebalance_kill"]
CONFIGS = ["A", "B"]


def load_runs():
    runs = []
    for path in sorted(glob.glob(os.path.join(REPO, "results", "runs", "*.json"))):
        with open(path) as fh:
            runs.append(json.load(fh))
    return runs


def enrich(rec):
    """Derived per-run metrics (Section 8), or None if not computable."""
    if rec.get("status") != "completed" or not rec.get("verifier"):
        return None
    src = rec.get("source_summary") or {}
    procs = [rec.get("proc1_summary"), rec.get("proc2_summary")]
    done = [p for p in procs if p and p.get("status") == "completed"]
    if not src or not done or src.get("t_first_send") is None:
        return None
    n = rec["n"]
    prod_span_s = (src["t_end"] - src["t_first_send"]) / 1000.0
    proc_end = max(p["t_end"] for p in done)
    makespan_s = (proc_end - src["t_first_send"]) / 1000.0
    ver = rec["verifier"]
    return {
        "runid": rec["runid"],
        "producer_tput": n / prod_span_s if prod_span_s > 0 else None,
        "pipeline_tput": n / makespan_s if makespan_s > 0 else None,
        "makespan_s": makespan_s,
        "duplicates": ver["duplicates"],
        "losses": ver["losses"],
        "out_of_order": ver["out_of_order"],
        "unique": ver["unique"],
        "latency": ver["latency_ms"],
        "recovery_ms": (rec.get("broker_recovery") or {}).get("recovery_ms"),
        "ack_gap_ms": rec.get("ack_gap_ms"),
    }


def cell_runs(runs, config, scenario):
    """Scored runs for one cell: for each rep, the completed record if
    one exists, else the best available record (for status display)."""
    by_rep = {}
    for rec in runs:
        if rec["config"] == config and rec["scenario"] == scenario:
            by_rep.setdefault(rec["rep"], []).append(rec)
    scored, display = {}, {}
    for rep, recs in by_rep.items():
        good = [r for r in recs if enrich(r) is not None]
        if good:
            scored[rep] = enrich(good[0])
            display[rep] = good[0]
        else:
            display[rep] = recs[-1]
    return scored, display


def median(vals):
    vals = [v for v in vals if v is not None]
    return statistics.median(vals) if vals else None


def main():
    runs = load_runs()
    cells = {}
    for config in CONFIGS:
        for scenario in SCENARIOS:
            cells[(config, scenario)] = cell_runs(runs, config, scenario)

    # H1
    b_fault_ok = True
    b_fault_bad = False
    b_fault_incomplete = False
    for scenario in ["consumer_kill", "broker_kill", "rebalance_kill"]:
        scored, _ = cells[("B", scenario)]
        if len(scored) < 3:
            b_fault_incomplete = True
        for m in scored.values():
            if m["duplicates"] > 0 or m["losses"] > 0:
                b_fault_bad = True
            else:
                pass
    a_ck_scored, _ = cells[("A", "consumer_kill")]
    a_dup_runs = sum(1 for m in a_ck_scored.values() if m["duplicates"] > 0)
    if b_fault_incomplete or len(a_ck_scored) < 3:
        h1 = "INCONCLUSIVE"
    elif b_fault_bad or a_dup_runs <= 1:
        h1 = "REFUTED"
    else:
        h1 = "CONFIRMED"

    # H2
    a_base, _ = cells[("A", "baseline")]
    b_base, _ = cells[("B", "baseline")]
    a_tput = median([m["pipeline_tput"] for m in a_base.values()])
    b_tput = median([m["pipeline_tput"] for m in b_base.values()])
    if len(a_base) < 3 or len(b_base) < 3 or a_tput is None or b_tput is None:
        h2 = "INCONCLUSIVE"
        h2_ratio = None
    else:
        h2_ratio = b_tput / a_tput
        h2 = "CONFIRMED" if h2_ratio <= 0.80 else "REFUTED"

    # H3
    h3_incomplete = False
    h3_bad = False
    recoveries = []
    for config in CONFIGS:
        scored, _ = cells[(config, "broker_kill")]
        if len(scored) < 3:
            h3_incomplete = True
        for m in scored.values():
            if m["losses"] > 0:
                h3_bad = True
            if m["recovery_ms"] is not None:
                recoveries.append(m["recovery_ms"])
    if h3_incomplete:
        h3 = "INCONCLUSIVE"
    elif h3_bad:
        h3 = "REFUTED"
    else:
        h3 = "CONFIRMED"

    summary = {
        "verdicts": {"H1": h1, "H2": h2, "H3": h3},
        "h2_ratio": h2_ratio,
        "baseline_pipeline_tput_median": {"A": a_tput, "B": b_tput},
        "broker_recovery_ms_median": median(recoveries),
        "a_consumer_kill_dup_runs": a_dup_runs,
        "cells": {
            f"{c}-{s}": {
                "scored_reps": sorted(cells[(c, s)][0].keys()),
                "metrics": cells[(c, s)][0],
            }
            for c in CONFIGS
            for s in SCENARIOS
        },
    }
    with open(os.path.join(REPO, "results", "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=1, sort_keys=True)

    def fnum(x, digits=1):
        return "n/a" if x is None else f"{x:,.{digits}f}"

    lines = []
    lines.append("# Results: Kafka Streaming Lab")
    lines.append("")
    lines.append("Generated by scripts/make_results.py from results/runs/*.json.")
    lines.append("Verdict rules: PREREGISTRATION.md Section 9. Metric definitions: Section 8.")
    lines.append("")
    lines.append("## Verdicts")
    lines.append("")
    lines.append(f"- H1 (semantics are real, both directions): **{h1}**")
    lines.append(f"- H2 (exactly-once costs throughput, ratio at most 0.80): **{h2}**"
                 + (f" (measured ratio {h2_ratio:.3f})" if h2_ratio is not None else ""))
    lines.append(f"- H3 (no acknowledged message lost across a broker kill): **{h3}**")
    lines.append("")
    lines.append("## Headline numbers (medians of 3 baseline repetitions)")
    lines.append("")
    lines.append("| Config | Pipeline throughput (msg/s) | Producer throughput (msg/s) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for config in CONFIGS:
        base, _ = cells[(config, "baseline")]
        pt = median([m["pipeline_tput"] for m in base.values()])
        pr = median([m["producer_tput"] for m in base.values()])
        p50 = median([m["latency"]["p50"] for m in base.values()])
        p95 = median([m["latency"]["p95"] for m in base.values()])
        p99 = median([m["latency"]["p99"] for m in base.values()])
        lines.append(
            f"| {config} | {fnum(pt)} | {fnum(pr)} | {fnum(p50, 2)} | {fnum(p95, 2)} | {fnum(p99, 2)} |"
        )
    lines.append("")
    lines.append("## Correctness counts, every run")
    lines.append("")
    lines.append("| Run | Status | Duplicates | Losses | Out of order | Unique | Broker recovery (ms) | Ack gap (ms) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for config in CONFIGS:
        for scenario in SCENARIOS:
            scored, display = cells[(config, scenario)]
            for rep in sorted(display.keys()):
                rec = display[rep]
                m = scored.get(rep)
                if m is not None:
                    lines.append(
                        f"| {rec['runid']} | completed | {m['duplicates']} | {m['losses']} | "
                        f"{m['out_of_order']} | {m['unique']} | {fnum(m['recovery_ms'])} | {fnum(m['ack_gap_ms'])} |"
                    )
                else:
                    lines.append(
                        f"| {rec['runid']} | {rec['status']} | | | | | | |"
                    )
    lines.append("")
    lines.append("## Per-run performance")
    lines.append("")
    lines.append("| Run | Pipeline throughput (msg/s) | Producer throughput (msg/s) | Makespan (s) | Latency p50 (ms) | p95 (ms) | p99 (ms) |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- |")
    for config in CONFIGS:
        for scenario in SCENARIOS:
            scored, _ = cells[(config, scenario)]
            for rep in sorted(scored.keys()):
                m = scored[rep]
                lines.append(
                    f"| {m['runid']} | {fnum(m['pipeline_tput'])} | {fnum(m['producer_tput'])} | "
                    f"{fnum(m['makespan_s'], 2)} | {fnum(m['latency']['p50'], 2)} | "
                    f"{fnum(m['latency']['p95'], 2)} | {fnum(m['latency']['p99'], 2)} |"
                )
    lines.append("")
    with open(os.path.join(REPO, "RESULTS.md"), "w") as fh:
        fh.write("\n".join(lines))
    print(json.dumps(summary["verdicts"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())

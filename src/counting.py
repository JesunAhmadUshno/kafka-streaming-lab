"""Correctness counting for the output topic, per PREREGISTRATION.md
Sections 2 and 8. Pure functions over the verifier's ordered record
list, so they can be unit tested without a broker.

records: list of (seq, t_send_ms, t_out_ms) in arrival order, data
records only (seq >= 0).
acked: set of sequence numbers the source producer saw acknowledged.
"""


def percentile(sorted_values, q):
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = q * (len(sorted_values) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def count_correctness(records, acked):
    seen = {}
    latencies = []
    duplicates = 0
    out_of_order = 0
    max_seen = None
    for seq, t_send, t_out in records:
        if max_seen is not None and seq < max_seen:
            out_of_order += 1
        if max_seen is None or seq > max_seen:
            max_seen = seq
        if seq in seen:
            duplicates += 1
        else:
            seen[seq] = (t_send, t_out)
            latencies.append(t_out - t_send)
    lost = sorted(s for s in acked if s not in seen)
    latencies.sort()
    return {
        "records_total": len(records),
        "unique": len(seen),
        "duplicates": duplicates,
        "losses": len(lost),
        "lost_seqs_sample": lost[:20],
        "out_of_order": out_of_order,
        "latency_ms": {
            "p50": percentile(latencies, 0.50),
            "p95": percentile(latencies, 0.95),
            "p99": percentile(latencies, 0.99),
        },
    }

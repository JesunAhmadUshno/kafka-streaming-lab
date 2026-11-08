# Kafka Streaming Lab

What do Kafka's delivery semantics actually cost, and what do they
actually guarantee when processes die mid-stream? This study kills
consumers and the broker in the middle of a 20,000-message stream
and counts duplicates and losses at the output topic, instead of
trusting the manual. Hypotheses and thresholds were frozen in
PREREGISTRATION.md before any run; verdicts are computed by
scripts/make_results.py from the run records.

## Verdicts

- **H1 CONFIRMED.** The exactly-once configuration (B) produced
  zero duplicates and zero losses in all 9 of its fault runs. The
  at-least-once configuration (A) produced duplicates in 3 of its 3
  consumer-kill runs: 5,141, 8,805 and 5,349 duplicate records out
  of 20,000.
- **H2 CONFIRMED.** Exactly-once costs throughput. Median baseline
  pipeline throughput: A 5,264.5 messages per second, B 3,337.8.
  The ratio is 0.634, below the frozen 0.80 line.
- **H3 CONFIRMED.** No acknowledged message was lost in any of the
  6 broker-kill runs. Median broker recovery time was 22,406.5 ms.
  Scope, stated plainly: replication factor 1 means acks = all is
  only as strong as the one node's disk. A process kill with the
  disk intact is what was tested; disk loss or machine loss is not
  tested and is not promised by this configuration.

## Headline numbers (baseline medians of 3)

| Config | Pipeline throughput (msg/s) | Producer throughput (msg/s) | Latency p50 (ms) | Latency p95 (ms) | Latency p99 (ms) |
| --- | --- | --- | --- | --- | --- |
| A (at-least-once) | 5,264.5 | 48,780.5 | 3,305 | 3,366 | 3,380 |
| B (exactly-once) | 3,337.8 | 28,818.4 | 4,562 | 5,715 | 5,776 |

Latency is measured in-band: the source stamps each message at its
produce call, the processor stamps it again when it re-emits it,
and the verifier reports the difference. It is pipeline latency
through a single-threaded Python stage, not broker transit time;
see AUDIT.md for the observed pattern and its limits.

Full per-run tables, correctness counts for all 24 runs, and
per-run injection notes are in RESULTS.md.

## What the fault runs showed

- **Consumer kill, configuration A:** the restarted consumer
  reprocessed everything its periodic offset commits had not yet
  covered. Duplicates: 5,141, 8,805 and 5,349. Losses: zero. The
  pipeline also stalled for about 45 seconds before the partition
  was reassigned, matching the client default session timeout of
  45 s: a hard kill is only noticed when the session expires.
- **Consumer kill, configuration B:** the restarted processor,
  reusing the same transactional.id, resumed from the offsets
  committed with its transactions. Duplicates: zero. Losses: zero.
  The same roughly 45 s reassignment stall applies.
- **Broker kill, both configurations:** the broker was killed with
  docker kill (SIGKILL) and restarted on the same volume. Recovery
  to a successful metadata call took 22,401 to 22,493 ms. In
  configuration A the producer sat without an acknowledgement for
  22,801 to 28,469 ms and still lost nothing it had acknowledged;
  one A run re-delivered 292 records (duplicates), the other two
  none. In configuration B the kill landed after the fast source
  had already finished, so it stress-tested the transactional
  processor mid-stream: its makespan stretched from about 6 s to
  about 30.5 s, and it still delivered every acknowledged message
  exactly once.
- **Rebalance kill:** for B, the second group member's arrival
  killed the first member inside the window, once by SIGKILL and
  twice by transactional fencing (the second member initialised
  the shared transactional.id and the broker fenced the first,
  error _FENCED); the survivor completed the stream with zero
  duplicates and zero losses. For A, the first processor finished
  the whole stream inside the frozen 1.0 s window in all 3 runs,
  so the kill was never delivered; that cell measured a late group
  join only, and RESULTS.md says so on the cell.

## Why this matters beyond the lab

Payment events, sensor feeds and audit logs ride on exactly these
semantics. A duplicate is a double charge or a double-counted
reading; a loss is a payment with no record or a gap in an audit
trail. The difference between the two configurations in this study
is not a tuning detail: under a routine consumer crash, the
at-least-once pipeline re-delivered a quarter to nearly a half of
the stream, and the exactly-once pipeline re-delivered none of it,
at about 63 percent of the throughput. Those are the terms of the
trade, measured under failure rather than assumed from a manual.

## Design in one paragraph

A source producer writes 20,000 sequenced 256-byte messages plus a
sentinel to a single-partition input topic; a processor copies each
message to an output topic; a verifier scores the output topic from
the sequence numbers alone. Configuration A: acks = all with
retries, no idempotence, auto-committing consumer. Configuration
B: idempotent source, read-committed consumer, transactional
processor committing output writes and offsets together in batches
of at most 100. Four scenarios (baseline, consumer kill, broker
kill, rebalance kill), 3 repetitions each, 24 runs. Broker: Apache
Kafka 3.9.0, single-node KRaft, in Docker with a 1 GiB memory and
2 CPU limit and a 512 MiB heap cap. Machine of record: BlackBoxV2
(Amendment A1 in PREREGISTRATION.md explains why the originally
named machine could not run the broker at all, with the evidence).
Clients: confluent-kafka 2.16.1 (librdkafka 2.16.0). Exact
versions: results/versions.json.

## Reproduce

```bash
docker compose up -d            # broker, apache/kafka:3.9.0
python3 -m pip install -r requirements.txt
python3 scripts/run_matrix.py   # 24 runs, writes results/runs/
python3 scripts/ack_gap_analysis.py
python3 scripts/make_results.py # RESULTS.md and results/summary.json
python3 -m pytest               # counting and percentile tests
```

## Files

- PREREGISTRATION.md: the frozen design, hypotheses and thresholds,
  plus Amendment A1 (machine of record).
- RESULTS.md: generated results, verdicts, all 24 runs.
- results/runs/: one JSON record per run.
- results/ack_gaps.json: acknowledgement-gap analysis for the
  broker-kill runs, from the raw acknowledgement logs. The raw
  logs themselves are in results/ack-logs/ (gzipped), so the
  analysis can be recomputed from this repository alone.
- results/jev-claim-check.json: the Jev claim_check gate record
  for the headline verdicts (REVIEW route, hand-verified).
- DECISIONS.md: build decisions D1 to D5.
- AUDIT.md: the grade, itemised deductions, and what was verified.
- src/: source producer, processor, verifier, shared codec.
- scripts/: matrix driver, results generator, ack-gap analysis,
  version recorder.
- docker-compose.yml and conf/server.properties.template: the
  broker stack and its properties.

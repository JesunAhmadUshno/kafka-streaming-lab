# Audit: Kafka Streaming Lab

Grade: **6,200 of 10,000**.

All three hypotheses were resolved under the frozen rules
(H1, H2, H3 all CONFIRMED), the matrix ran complete (24 of 24 runs
completed, zero timeouts, zero runs lost to harness failure), and
every published figure traces to a run record. The deductions
below are the price of the degraded cells, the corrected metric,
and the stated limits. Nothing here is rounded up.

## Deductions

- **-900: The A rebalance_kill cell never received its kill.**
  In all 3 runs the first processor completed the whole 20,000
  message stream inside the frozen 1.0 s window between the second
  member starting and the scheduled SIGKILL. The cell measured a
  late group join, not a kill during rebalance. It is labelled as
  such on the cell in RESULTS.md. No hypothesis depends on this
  cell, but the fault matrix as designed is incomplete there.
- **-500: The B broker_kill trigger lagged the source.** The
  configuration B source acknowledges all 20,001 messages in under
  a second, faster than the driver's progress check could observe
  the 10,000 threshold, so in all 3 runs the broker was killed
  after the source had finished. The kill genuinely interrupted
  the transactional processor mid-stream (makespan about 6 s to
  about 30.5 s, zero losses of the fully acknowledged stream),
  but a mid-source-stream broker kill for configuration B was not
  measured. Proven from the raw ack logs: longest ack gap 1 ms.
- **-500: The latency metric is pipeline latency, and its baseline
  shape is dominated by a fixed start offset.** Latency is stamped
  in-band (source produce call to processor re-emission), so it
  excludes the output delivery hop, and in baseline runs the
  distribution sits close to the makespan: consumption begins only
  after a fixed group-formation and fetch-start delay on a fresh
  group and topic, then drains in a burst. The same roughly 2.2 s
  offset appears in a 300-message smoke run, where drain time is
  negligible. The numbers are reported as defined; they must not
  be read as broker transit times.
- **-400: The machine of record had to move mid-build.** The
  originally named VM cannot run the Kafka broker at all (six
  starts, identical controller-registration failure, verified with
  Java and C clients; see Amendment A1 for the full evidence).
  The amendment was recorded before any measured run and restored
  the originally briefed Docker deployment, but a study that has
  to change machines before it can start pays for the disruption
  and for the environment claim in its first pre-registration.
- **-400: The driver's first-pass ack-gap metric was wrong.** It
  measured across the driver's trigger timestamp, which precedes
  the actual container kill, and printed 1 to 3 ms artifacts for
  configuration A and nulls for B. The published tables use the
  corrected quantity (longest gap between consecutive successful
  acknowledgements) recomputed from the raw acknowledgement logs
  by scripts/ack_gap_analysis.py, and the driver code now computes
  the same definition. The flaw was caught in verification, before
  publication; it is charged anyway.
- **-300: B rebalance_kill delivered its kill by fencing, not by
  SIGKILL, in 2 of 3 runs.** When the second member initialised
  the shared transactional.id, the broker fenced the first member
  (fatal error _FENCED) before the scheduled kill landed. The
  first member did die inside the rebalance window and the counts
  are genuine, but the mechanism differed from the frozen
  schedule in those runs. Disclosed on the cell in RESULTS.md.
- **-300: Generality is capped by the frozen simplifications.**
  One partition and replication factor 1 make the counts exact
  and the ordering total; they also mean the throughput figures
  are single-partition figures and the broker-kill guarantee is a
  single-disk guarantee. Registered in the pre-registration
  (Sections 3 and 9), priced here.
- **-300: Co-tenancy noise on the machine of record.** BlackBoxV2
  runs other services and none was stopped. The first A baseline
  run (2,974 msg/s) sits well below its siblings (5,264 and
  5,467), and the spread is visible in the tables rather than
  averaged away, but timing precision on a shared machine is
  limited.
- **-200: Broker recovery time is a single composite number.**
  The 22,401 to 22,493 ms recovery is measured exactly as frozen
  (admin metadata probe every 250 ms, minus the 125 ms midpoint
  correction) and is strikingly uniform, but it is not decomposed
  into container start, JVM boot, and metadata catch-up in the
  published tables.

Total deducted: 3,800. Grade: 6,200 of 10,000.

## What was verified by hand

- All 24 run records were re-read directly. Every run's verifier
  counted acked_total 20,000 of 20,000; every source reported 0
  delivery errors and 0 unflushed messages; every run's unique
  count is 20,000.
- H1: configuration B fault cells show duplicates 0 and losses 0
  in all 9 runs; configuration A consumer_kill duplicates are
  5,141, 8,805 and 5,349 (3 of 3 runs with duplicates).
- H2: ratio recomputed from the baseline medians,
  3,337.8 / 5,264.5 = 0.634, at or below the frozen 0.80 line.
- H3: losses are 0 in all 6 broker-kill runs; recovery median
  recomputed from the six recorded values as 22,406.5 ms.
- The rebalance and broker-kill injection notes in RESULTS.md were
  each traced to the run record's event list, exit codes, and
  processor summaries (including the _FENCED error text in the
  B rebalance runs and the completed-alone summaries in the
  A rebalance runs).
- The corrected ack gaps were recomputed from the raw
  acknowledgement logs on the machine of record (A: 22,801,
  28,469, 23,505 ms; B: 1 ms in all 3 runs).

## Jev claim_check

- Gate: claim_check (standing bands: clear at 0.85 or above,
  block at 0.10 or below, review between).
- Model: jev-1.13.0. Question: supported. Noul: 0.41.
  Route: **REVIEW**. Usage: 958 input tokens, 20 output tokens.
- Spec: ~/workspace/tools/jev/kafka-streaming-lab-claim-spec-2026-11-08.json
- Raw result: ~/workspace/tools/jev/kafka-streaming-lab-claim-results-2026-11-08.json
- Repo record: results/jev-claim-check.json.
- On the REVIEW route, every figure in the claim was hand-verified
  against the run records and results/summary.json, as itemised
  above. All figures agree with the claim as stated.

## Amendments and deviations register

- Amendment A1 (PREREGISTRATION.md) and DECISIONS.md D5: machine
  of record moved from the Muse VM (htch-runtime) to BlackBoxV2,
  and the broker runs in Docker as originally briefed. Recorded
  before any measured run. D1 (user-space JVM broker) is
  superseded by D5.
- D4 (127.0.0.1 listener binding) was a harness correction from
  the failed bring-up on the original machine; the same binding is
  used for the published port and client bootstrap on BlackBoxV2.
- No frozen element changed: workload, both configurations, fault
  schedule, metric definitions, hypothesis thresholds, and verdict
  rules are exactly as locked in the first content commit
  (027ef11) plus Amendment A1.

## What a second version should fix

- Slow the stream or move the rebalance trigger earlier so the
  configuration A rebalance kill actually lands inside the window.
- Poll the source progress faster (or trigger on the ack log
  directly) so the broker kill lands mid-source-stream for fast
  producers.
- Add broker-side timestamps to the latency metric so pipeline
  latency and broker transit can be reported separately.

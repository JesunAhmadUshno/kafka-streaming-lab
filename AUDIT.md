# Audit: Kafka Streaming Lab

This file is the report in the portfolio standard. It records what
was verified, how the evidence is protected, every deviation from the
locked plan, and the threats to validity. Grades are kept outside the
public repository.

All three hypotheses were resolved under the frozen rules
(H1, H2, H3 all CONFIRMED), the matrix ran complete (24 of 24 runs
completed, zero timeouts, zero runs lost to harness failure), and
every published figure traces to a run record. The deviations and
limits below are the degraded cells, the corrected metric, and the
stated limits. Nothing here is rounded up.

## Deviations and limits on record

- **The A rebalance_kill cell never received its kill.**
  In all 3 runs the first processor completed the whole 20,000
  message stream inside the frozen 1.0 s window between the second
  member starting and the scheduled SIGKILL. The cell measured a
  late group join, not a kill during rebalance. It is labelled as
  such on the cell in RESULTS.md. No hypothesis depends on this
  cell, but the fault matrix as designed is incomplete there.
- **The B broker_kill trigger lagged the source.** The
  configuration B source acknowledges all 20,001 messages in under
  a second, faster than the driver's progress check could observe
  the 10,000 threshold, so in all 3 runs the broker was killed
  after the source had finished. The kill genuinely interrupted
  the transactional processor mid-stream (makespan about 6 s to
  about 30.5 s, zero losses of the fully acknowledged stream),
  but a mid-source-stream broker kill for configuration B was not
  measured. Proven from the raw ack logs: longest ack gap 1 ms.
- **The latency metric is pipeline latency, and its baseline
  shape is dominated by a fixed start offset.** Latency is stamped
  in-band (source produce call to processor re-emission), so it
  excludes the output delivery hop, and in baseline runs the
  distribution sits close to the makespan: consumption begins only
  after a fixed group-formation and fetch-start delay on a fresh
  group and topic, then drains in a burst. The same roughly 2.2 s
  offset appears in a 300-message smoke run, where drain time is
  negligible. The numbers are reported as defined; they must not
  be read as broker transit times.
- **The machine of record had to move mid-build.** The
  originally named VM cannot run the Kafka broker at all (six
  starts, identical controller-registration failure, verified with
  Java and C clients; see Amendment A1 for the full evidence).
  The amendment was recorded before any measured run and restored
  the originally briefed Docker deployment, but a study that has
  to change machines before it can start is disrupted, and the
  environment claim in its first pre-registration did not hold.
- **The driver's first-pass ack-gap metric was wrong.** It
  measured across the driver's trigger timestamp, which precedes
  the actual container kill, and printed 1 to 3 ms artifacts for
  configuration A and nulls for B. The published tables use the
  corrected quantity (longest gap between consecutive successful
  acknowledgements) recomputed from the raw acknowledgement logs
  by scripts/ack_gap_analysis.py, and the driver code now computes
  the same definition. The flaw was caught in verification, before
  publication; it is recorded here anyway.
- **B rebalance_kill delivered its kill by fencing, not by
  SIGKILL, in 2 of 3 runs.** When the second member initialised
  the shared transactional.id, the broker fenced the first member
  (fatal error _FENCED) before the scheduled kill landed. The
  first member did die inside the rebalance window and the counts
  are genuine, but the mechanism differed from the frozen
  schedule in those runs. Disclosed on the cell in RESULTS.md.
- **Generality is capped by the frozen simplifications.**
  One partition and replication factor 1 make the counts exact
  and the ordering total; they also mean the throughput figures
  are single-partition figures and the broker-kill guarantee is a
  single-disk guarantee. Registered in the pre-registration
  (Sections 3 and 9), restated here.
- **Co-tenancy noise on the machine of record.** BlackBoxV2
  runs other services and none was stopped. The first A baseline
  run (2,974 msg/s) sits well below its siblings (5,264 and
  5,467), and the spread is visible in the tables rather than
  averaged away, but timing precision on a shared machine is
  limited.
- **Broker recovery time is a single composite number.**
  The 22,401 to 22,493 ms recovery is measured exactly as frozen
  (admin metadata probe every 250 ms, minus the 125 ms midpoint
  correction) and is strikingly uniform, but it is not decomposed
  into container start, JVM boot, and metadata catch-up in the
  published tables.

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

## How the evidence is protected

- The plan came first. PREREGISTRATION.md was the first commit in
  this repository (027ef11, 18:06 UTC on 2026-11-08), before any
  harness file or result. That commit is the lock (tag proposal-v1).
  Amendment A1 was appended in acc54de (19:38 UTC), and it only adds
  text: the 11,457 bytes of the locked file are unchanged at the top
  of the current file. The run records' own start timestamps
  (t_start in results/runs/*.json) put the 24 measured runs between
  19:41 and 19:56 UTC, after the amendment commit.
- The lock is enforced in code. tests/test_layout.py pins two
  SHA-256 hashes: the locked text as committed in 027ef11, which
  must still open the file byte for byte, and the whole file with
  Amendment A1 (acc54de). Any further edit fails CI; a new
  amendment has to change the pin in a reviewed pull request.
- Verdict rules did not move after the data. The one change to
  scripts/make_results.py after the runs (26fd549) added the
  corrected ack-gap column and the per-run injection notes; the
  code that applies the H1, H2 and H3 rules is unchanged from the
  harness commit (8fca9c7).
- Results regenerate from the raw evidence in CI. Every pull request
  reruns scripts/ack_gap_analysis.py over the gzipped raw
  acknowledgement logs in results/ack-logs/ and
  scripts/make_results.py over results/runs/, then fails if
  results/ack_gaps.json, results/summary.json or RESULTS.md differ
  by one byte from what is committed.
- Every registered run is reported. All 24 records are in
  results/runs/, including the three A rebalance_kill runs whose
  kill was never delivered; no run was retried until it passed.
- No secret is in the repository. The broker runs on 127.0.0.1 only
  and has no credentials. The Jev claim_check record
  (results/jev-claim-check.json) holds the route, score, token
  counts and the hand verification, and no API key.

## Threats to validity

- The H1 INCONCLUSIVE clause and the A rebalance_kill cell. H1 is
  INCONCLUSIVE "if any fault cell is injection-failed or incomplete
  for either configuration". Section 6 defines injection failure as
  the trigger never firing, or the first processor finishing before
  the trigger fires. In the three A rebalance_kill runs the trigger
  did fire (5,000 consumed, second member started), and the first
  processor finished inside the 1.0 s wait, so the SIGKILL had no
  process to land on. The generator scores those runs as completed,
  not injection_failed, and no replacement runs were made. That is
  the literal reading of Section 6. A stricter reading, that a kill
  which never lands is a failed injection, would make H1
  INCONCLUSIVE, not CONFIRMED. Both clauses H1 actually tests (B
  fault cells clean, A consumer_kill duplicates) hold either way;
  the open question is only the INCONCLUSIVE gate, and it is stated
  here rather than settled quietly.
- Internal validity: the B broker_kill kill landed after the source
  had finished, so H3 for configuration B covers a kill during the
  processor stage, not during the source stream (see above).
- Construct validity: latency is pipeline latency through one Python
  stage, not broker transit time, and baseline latency is dominated
  by a fixed group-formation offset.
- External validity: one partition and replication factor 1. The
  throughput figures are single-partition figures, and H3 is a
  single-disk guarantee against a process kill, not against disk or
  machine loss.
- Measurement: BlackBoxV2 is shared and no co-tenant was stopped;
  load was not recorded per run. The first A baseline run sits well
  below its siblings, and every run is shown.
- Sample size: three repetitions per cell. Medians are reported with
  every repetition printed; no interval is claimed.
- Reliability profile gaps: the study states a data-loss target
  (zero acknowledged losses, H3) but no recovery-time target, and it
  has no monitoring, alerting, runbook or postmortem. Recovery time
  (about 22.4 s) is measured, not judged against a target.

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

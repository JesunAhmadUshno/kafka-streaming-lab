# Pre-registration: Kafka Streaming Lab

Locked before any measured run exists. The commit that adds this file is
the first content commit in this repository and precedes every harness
file and every result; git history proves the order. Any change to this
file after locking is an amendment and must be recorded in DECISIONS.md
with its reason, before the runs it affects.

## 1. Research question

On a single-node Apache Kafka, what do the delivery semantics actually
cost and guarantee under failure? Concretely: how do an at-least-once
configuration and an exactly-once configuration differ in measured
throughput, end-to-end latency, and duplicate and loss counts when the
consumer process and the broker are killed mid-stream?

Manuals state the semantics. This study counts them.

## 2. Pipeline under test

A three-stage pipeline, identical in shape for both configurations:

1. A source producer writes N sequenced messages to an input topic,
   then one sentinel message (seq = -1) on the same partition.
2. A processor consumes the input topic and writes one output record
   per input message to an output topic, carrying the sequence number
   and both timestamps. The processor finishes when it has processed
   the sentinel.
3. A verifier reads the whole output topic and counts duplicates,
   losses, and out-of-order arrivals from the sequence numbers alone,
   and computes end-to-end latency percentiles from the timestamps.

Correctness is scored at the output topic, never from the processor's
own claims. A record counts as a duplicate when its sequence number
was already seen earlier in the output topic. A record counts as lost
when the source producer received an acknowledgement for its sequence
number and that number never appears in the output topic after the
pipeline has fully drained. An out-of-order event is an arrival whose
sequence number is smaller than the largest number already seen.

## 3. Workload (frozen)

- N = 20,000 data messages per run, plus one sentinel.
- Value size: exactly 256 bytes per message. The value is a JSON
  header (sequence number, producer send timestamp in milliseconds)
  padded with a filler string to 256 bytes.
- Input and output topics: 1 partition, replication factor 1.
  One partition is a deliberate simplification, registered here: it
  makes the expected order total, so reordering is countable. It
  caps parallelism, so all throughput figures are single-partition
  figures and are labelled as such.
- The source producer sends as fast as the client library allows
  (no artificial pacing), then flushes and writes its acknowledgement
  log.

## 4. Configurations (frozen)

Both configurations use the confluent-kafka Python client (librdkafka),
pinned in requirements.txt; the exact version is recorded in
results/versions.json. Settings not listed here stay at client
defaults in both configurations.

Configuration A, at-least-once:
- Source and processor output producers: acks = all, retries = 10,
  enable.idempotence = false, message.timeout.ms = 120000.
- Processor consumer: enable.auto.commit = true,
  auto.commit.interval.ms = 1000, enable.auto.offset.store = true,
  isolation.level = read_uncommitted.
- The processor produces each output record asynchronously and
  flushes its producer only at the sentinel.

Configuration B, exactly-once:
- Source producer: enable.idempotence = true, acks = all,
  retries = 10. (The source writes no transactions; it is an origin,
  and idempotence is what its retries buy. Registered openly.)
- Processor consumer: enable.auto.commit = false,
  isolation.level = read_committed.
- Processor output producer: enable.idempotence = true, acks = all,
  transactional.id = a run-scoped unique id, transaction.timeout.ms
  = 120000.
- The processor works in batches of at most 100 consumed messages:
  begin transaction, produce the batch's output records, send the
  consumer offsets to the transaction, commit the transaction. The
  sentinel's batch commits the same way, after which the processor
  exits.
- A restarted processor reuses the same transactional.id, so a
  zombie instance is fenced by the broker.

## 5. Broker (frozen), and a declared deviation

Broker: Apache Kafka 3.9.0 (Scala 2.13), KRaft mode, one node acting
as broker and controller. Frozen broker properties: offsets topic
replication factor 1, transaction state log replication factor 1,
transaction state log min ISR 1, default replication factor 1,
min.insync.replicas 1, auto topic creation off, log retention long
enough that no run can age out. Heap capped at 512 MiB. The identical
properties are in server.properties.template in this repository and in
the shipped docker-compose.yml.

Declared deviation, registered before any run: the machine of record
(Section 7) has no container runtime (no Docker, Podman, or nerdctl
binary and no Docker socket, verified 2026-11-08). The brief asked
for Kafka in Docker. On this machine the broker therefore runs as a
user-space JVM process from the official Apache Kafka 3.9.0 release
tarball (SHA-512 verified against the publisher's checksum file)
under Eclipse Temurin JRE 21.0.13, also a user-space install. The
broker kill fault is SIGKILL to the broker process, and the broker's
log directory plays the role of the container volume: it persists
across the kill, exactly like a named volume across a container
restart. The docker-compose.yml in this repository carries the same
broker properties for reproduction on hosts that have Docker. The
deviation changes the packaging, not the protocol under test, and it
is charged in the audit grade.

## 6. Scenarios and fault schedule (frozen)

Four scenarios, each run for both configurations, 3 repetitions each:
24 runs in total. Every run uses fresh run-scoped topics, deleted
afterwards. A run that exceeds 600 seconds wall clock is aborted and
recorded as failed with its partial state preserved.

- baseline: no fault is injected.
- consumer_kill: when the processor's consumed count first reaches
  5,000 (25 percent of N), the driver sends SIGKILL to the processor,
  waits 5 seconds, and restarts the processor with the same
  configuration (and, for B, the same transactional.id). The source
  producer is not touched.
- broker_kill: when the source producer's acknowledged count first
  reaches 10,000 (50 percent of N), the driver sends SIGKILL to the
  broker process, waits 5 seconds, and restarts the broker with the
  same log directory. Producer and processor are not touched and must
  ride through on retries. Broker recovery time is measured by an
  admin probe: the driver attempts a topic metadata call every
  250 ms from the moment of the kill, and recovery time is the delay
  until the first successful call, minus the probe interval's
  midpoint correction of 125 ms. The first post-kill producer
  acknowledgement delay is also reported from the producer log.
- rebalance_kill: when the processor's consumed count first reaches
  5,000, the driver starts a second processor process in the same
  consumer group, waits 1.0 second (inside the rebalance the join
  triggers), then sends SIGKILL to the first processor. The second
  processor runs to completion. If the first processor has already
  finished before the trigger fires, the injection has failed.

Injection failure rule: if a fault trigger never fires (for example
the stream completes first), the run is recorded as
injection_failed, is not scored, and is replaced by one replacement
run, which is also recorded. If the replacement also fails to inject,
the cell stands as injection-failed in the results and any hypothesis
depending on it follows its INCONCLUSIVE rule.

## 7. Machine of record

All runs happen on one named machine: the Muse VM, hostname
htch-runtime (Linux 7.0.0-43-generic, AMD EPYC 9D25, 4 vCPUs,
7.7 GiB RAM, 1 GiB swap), Python 3.12.3. It is a shared machine:
other tenants run on it, and no co-tenant is stopped for this study.
Co-tenancy is a stated caveat on every timing. No number from any
other machine is reported in this study. Exact versions of every
component are recorded in results/versions.json.

## 8. Metrics (frozen definitions)

From each run's records (source acknowledgement log, processor
summary, verifier output), the analysis script computes:

- Producer throughput: N divided by the span from the first send call
  to the last acknowledgement, both from the source log.
- Pipeline makespan: the span from the source's first send timestamp
  to the processor's completion timestamp (sentinel processed and, in
  configuration B, its transaction committed). Pipeline throughput is
  N divided by the makespan.
- End-to-end latency: for each output record, its produce timestamp
  minus the source send timestamp of the same sequence number.
  Reported as p50, p95, p99 over the records the verifier saw first
  (one per sequence number).
- Correctness counts: duplicates (extra records beyond the first per
  sequence number), duplicates-affected runs (runs with duplicates
  greater than zero), losses (acknowledged sequence numbers absent
  from the output after drain), out-of-order events.
- Broker recovery time (broker_kill runs only), per Section 6.

Headline figures are medians across the 3 repetitions, with every
repetition printed in the results tables. A median of an incomplete
cell is never printed; incomplete cells are shown as incomplete.

## 9. Hypotheses (falsifiable, with thresholds)

- H1 (the semantics are real, in both directions). Configuration B
  produces zero duplicates and zero losses in every one of its 9
  fault runs (consumer_kill, broker_kill, rebalance_kill, 3 each),
  and configuration A produces duplicates greater than zero in at
  least 2 of its 3 consumer_kill runs. CONFIRMED if both clauses
  hold. REFUTED if configuration B shows any duplicate or loss in
  any fault run, or if configuration A shows duplicates in at most
  1 of 3 consumer_kill runs. INCONCLUSIVE if any fault cell is
  injection-failed or incomplete for either configuration.
- H2 (exactly-once costs throughput). The median baseline pipeline
  throughput of configuration B is at most 0.80 times the median
  baseline pipeline throughput of configuration A. CONFIRMED at or
  below 0.80, REFUTED above 0.80, INCONCLUSIVE if either baseline
  cell is incomplete.
- H3 (a single node keeps what it acknowledged, across a process
  kill). In all 6 broker_kill runs (3 per configuration), losses of
  acknowledged messages equal zero. CONFIRMED if all 6 runs complete
  with zero losses. REFUTED if any completed broker_kill run loses
  an acknowledged message. INCONCLUSIVE if any broker_kill run is
  incomplete or injection-failed. The scope of this claim is frozen
  here, honestly: replication factor 1 and min ISR 1 mean acks = all
  is only as strong as the one node's disk. A process kill with the
  disk intact is survivable by construction of the log; disk loss or
  machine loss is not tested and is not promised by this
  configuration, and the README must say so.

## 10. Verdicts and failure handling

Verdicts (CONFIRMED, REFUTED, INCONCLUSIVE) are computed by
scripts/make_results.py from the run records under the rules in
Section 9. Nothing is hand-typed. A broker or pipeline failure that
prevents a whole scenario cell from completing is reported as the
result for that cell, and the affected hypotheses follow their
INCONCLUSIVE rules; the study does not quietly shrink its matrix.

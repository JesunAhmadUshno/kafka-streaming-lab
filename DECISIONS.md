# Decisions

Decisions taken during the build, in the programme's house form.
Anything that changes how a run is executed is recorded here, before
the runs it affects, with its reason.

## D5: Machine of record moves to BlackBoxV2; Docker restored; D1 superseded

Recorded as Amendment A1 in PREREGISTRATION.md, before any measured
run. The Muse VM (htch-runtime) cannot run the Kafka broker: six
starts of the verified Kafka 3.9.0 tarball, across localhost,
127.0.0.1, and host-interface listeners, on ports 9092/9093 and
19092/19093, under load and on a quiet rebooted VM, all failed
identically at controller registration (the controller elects
itself and accepts TCP, but never answers the registration or
ApiVersions requests; Java and librdkafka clients fail the same
way; plain Python TCP echo works). The study therefore runs on
BlackBoxV2, which has Docker 29.9.2, and the broker runs in Docker
exactly as the brief asked (the shipped docker-compose.yml,
apache/kafka:3.9.0, 1 GiB and 2 CPU container limits, 512 MiB heap,
named volume). Broker kill is docker kill (SIGKILL by default) and
restart is docker start on the same volume. Client pins are
identical; the harness interpreter there is Python 3.14.4. D1 is
superseded by this decision. The full evidence trail is in
Amendment A1.

## D1: The broker runs as a user-space process, not in Docker [SUPERSEDED by D5]

The brief asked for Apache Kafka in Docker. The machine of record
has no container runtime: no Docker, Podman, or nerdctl binary and
no Docker socket (verified 2026-11-08 before the pre-registration
was locked, and declared there in Section 5). Installing a container
runtime system-wide was rejected: it is a heavier change to a shared
machine than the study needs, and it is not reversible by deleting a
directory. Instead the broker runs as a user-space JVM process from
the official Apache Kafka 3.9.0 release tarball (SHA-512 verified
against the publisher's checksum) under Eclipse Temurin JRE 21.0.13,
with the broker heap capped at 512 MiB as the resource limit. The
broker properties are identical to the shipped docker-compose.yml,
which remains the reproduction path on hosts that have Docker. The
broker kill fault is SIGKILL to the broker process, and the broker's
log directory plays the role of the container volume. This decision
is charged in the audit grade.

## D2: confluent-kafka, not kafka-python

The client library is confluent-kafka 2.16.1 (librdkafka 2.16.0),
pinned in requirements.txt. Reason: the exactly-once arm needs a
transactional producer with init_transactions,
send_offsets_to_transaction, and commit and abort, plus an idempotent
producer. kafka-python's transaction support is incomplete; librdkafka
implements the full protocol. The choice was made before any run and
is frozen in the pre-registration (Section 4).

## D3: The source producer in configuration B is idempotent, not transactional

Registered in the pre-registration (Section 4) so it is part of the
frozen design, and repeated here for visibility: the source is an
origin, not a consume-transform-produce stage, so it has no offsets
to commit atomically with its writes. Its retries are made safe by
enable.idempotence. The exactly-once claim of configuration B rests
on the processor stage, where offsets and output writes are
committed in one transaction, and is scored at the output topic by
the verifier.

## D4: Listeners and quorum voters bind 127.0.0.1, not localhost

During harness bring-up (before any measured run), the broker with
localhost listener and quorum addresses failed to register with its
own controller quorum: the registration channel timed out and the
broker exited fatally after about 60 seconds (broker log,
2026-11-08). On this VM, localhost name resolution is not a safe
transport for the controller channel. The listener, advertised
listener, and quorum voter addresses were changed to the literal
127.0.0.1 in conf/server.properties.template, and the harness
bootstrap address with them. No semantic property changed: same
ports, same replication and ISR settings, same single node. The
docker-compose.yml keeps localhost in its advertised listener,
which is the correct form for a Docker host port mapping; its
internal quorum voter already uses the service name. Kept for the
record after D5: on BlackBoxV2 the same 127.0.0.1 binding is used
for the published port and the client bootstrap.

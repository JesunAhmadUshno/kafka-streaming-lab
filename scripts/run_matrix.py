"""Matrix driver for the Kafka streaming lab.

Runs the single-node KRaft broker from the repository's
docker-compose.yml on the machine of record (PREREGISTRATION.md
Amendment A1, DECISIONS.md D5), then runs every (configuration,
scenario) cell for the frozen number of repetitions, injecting
faults on the frozen schedule (Section 6), and writes one run
record per run into results/runs/.

Usage: python3 scripts/run_matrix.py [--configs A B]
       [--scenarios baseline consumer_kill broker_kill rebalance_kill]
       [--reps 3]
The interpreter must have confluent-kafka importable (the repo
.venv, or PYTHONPATH pointing at an install of the pinned version).
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time

from confluent_kafka.admin import AdminClient, NewTopic

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORK_DIR = os.path.join(REPO, ".work")
RUNS_DIR = os.path.join(WORK_DIR, "runs")
BOOTSTRAP = "127.0.0.1:9092"
CONTAINER = "ksl-kafka"
COMPOSE_FILE = os.path.join(REPO, "docker-compose.yml")
N = 20000
RUN_TIMEOUT_S = 600

sys.path.insert(0, os.path.join(REPO, "src"))
from common import now_ms, write_json_atomic  # noqa: E402

logging.getLogger("ksl").addHandler(logging.NullHandler())
logging.getLogger("ksl").setLevel(logging.CRITICAL)

CHILD_ENV = dict(os.environ)
CHILD_ENV["PYTHONPATH"] = os.pathsep.join(p for p in sys.path if p)


def compose(*args, timeout=120):
    return subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE] + list(args),
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def admin():
    return AdminClient(
        {"bootstrap.servers": BOOTSTRAP, "logger": logging.getLogger("ksl")}
    )


def broker_ready(timeout_s=3):
    try:
        md = admin().list_topics(timeout=timeout_s)
        return md is not None
    except Exception:
        return False


def wait_broker_ready(deadline_s=120):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        if broker_ready():
            return True
        time.sleep(0.5)
    return False


class Broker:
    """The compose-managed broker container.

    kill() is docker kill, whose default signal is SIGKILL to the
    broker process (PREREGISTRATION.md Section 6). start() restarts
    the same container on the same named volume, which is the same
    log directory.
    """

    def fresh_cluster(self):
        compose("down", "-v", timeout=180)
        out = compose("up", "-d", timeout=300)
        if out.returncode != 0:
            print(out.stdout[-1000:], out.stderr[-1000:], flush=True)
            return False
        return wait_broker_ready(deadline_s=180)

    def start(self):
        out = subprocess.run(
            ["docker", "start", CONTAINER],
            capture_output=True, text=True, timeout=60,
        )
        if out.returncode != 0:
            out = compose("up", "-d", timeout=300)
            if out.returncode != 0:
                return False
        return wait_broker_ready()

    def kill(self):
        subprocess.run(
            ["docker", "kill", CONTAINER],
            capture_output=True, text=True, timeout=60,
        )

    def stop(self):
        subprocess.run(
            ["docker", "stop", CONTAINER],
            capture_output=True, text=True, timeout=120,
        )

    def snapshot_logs(self, path):
        try:
            out = subprocess.run(
                ["docker", "logs", CONTAINER],
                capture_output=True, timeout=60,
            )
            with open(path, "wb") as fh:
                fh.write(out.stdout + out.stderr)
        except Exception:
            pass


def create_topics(names):
    adm = admin()
    futures = adm.create_topics(
        [NewTopic(n, num_partitions=1, replication_factor=1) for n in names]
    )
    for fut in futures.values():
        fut.result(timeout=30)


def delete_topics(names):
    try:
        adm = admin()
        futures = adm.delete_topics(names)
        for fut in futures.values():
            fut.result(timeout=30)
    except Exception:
        pass


def read_json(path):
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception:
        return None


def recovery_probe(t_kill, sink):
    """Admin probe every 250 ms until the first success (Section 6)."""
    while True:
        t_attempt = now_ms()
        ok = broker_ready(timeout_s=1)
        if ok:
            sink["recovery_ms"] = (t_attempt - t_kill) - 125
            sink["recovered_at"] = t_attempt
            return
        if t_attempt - t_kill > 180000:
            sink["recovery_ms"] = None
            return
        time.sleep(0.25)


def spawn(script, extra_args, workdir, tag):
    out = open(os.path.join(workdir, tag + ".stdout.log"), "wb")
    cmd = [sys.executable, os.path.join(REPO, "src", script)] + extra_args
    return subprocess.Popen(
        cmd, cwd=REPO, stdout=out, stderr=subprocess.STDOUT, env=CHILD_ENV
    )


def processor_args(config, in_topic, out_topic, group, txn_id, workdir, tag):
    return [
        "--bootstrap", BOOTSTRAP,
        "--input-topic", in_topic,
        "--output-topic", out_topic,
        "--group", group,
        "--config", config,
        "--txn-id", txn_id,
        "--progress", os.path.join(workdir, tag + ".progress.json"),
        "--summary", os.path.join(workdir, tag + ".summary.json"),
    ]


def run_one(broker, config, scenario, rep, suffix=""):
    runid = f"{config}-{scenario}-r{rep}{suffix}"
    workdir = os.path.join(RUNS_DIR, runid)
    os.makedirs(workdir, exist_ok=True)
    in_topic = f"ksl-in-{runid}"
    out_topic = f"ksl-out-{runid}"
    group = f"ksl-group-{runid}"
    txn_id = f"ksl-{runid}-p"
    record = {
        "runid": runid,
        "config": config,
        "scenario": scenario,
        "rep": rep,
        "replacement": bool(suffix),
        "n": N,
        "events": [],
        "injection_failed": False,
        "status": "started",
    }

    create_topics([in_topic, out_topic])
    t_start = now_ms()
    record["t_start"] = t_start

    proc1 = spawn(
        "processor.py",
        processor_args(config, in_topic, out_topic, group, txn_id, workdir, "proc1"),
        workdir,
        "proc1",
    )
    source = spawn(
        "source_producer.py",
        [
            "--bootstrap", BOOTSTRAP,
            "--topic", in_topic,
            "--n", str(N),
            "--config", config,
            "--ack-log", os.path.join(workdir, "ack_log.jsonl"),
            "--progress", os.path.join(workdir, "source.progress.json"),
            "--summary", os.path.join(workdir, "source.summary.json"),
        ],
        workdir,
        "source",
    )
    procs = {"proc1": proc1}
    proc2 = None
    injected = scenario == "baseline"
    kill_at = None
    restart_at = None
    recovery = {}
    probe_thread = None
    deadline = time.time() + RUN_TIMEOUT_S
    completed = False

    while time.time() < deadline:
        src_prog = read_json(os.path.join(workdir, "source.progress.json")) or {}
        p1_prog = read_json(os.path.join(workdir, "proc1.progress.json")) or {}
        p2_prog = read_json(os.path.join(workdir, "proc2.progress.json")) or {}
        consumed = max(p1_prog.get("consumed", 0), p2_prog.get("consumed", 0))
        acked = src_prog.get("acked", 0)

        if scenario == "consumer_kill" and not injected and consumed >= 5000:
            if proc1.poll() is not None:
                record["injection_failed"] = True
                record["events"].append({"event": "processor_finished_before_trigger"})
                break
            os.kill(proc1.pid, signal.SIGKILL)
            kill_at = now_ms()
            restart_at = time.time() + 5
            injected = True
            record["events"].append({"event": "consumer_killed", "t": kill_at, "consumed": consumed})
        if scenario == "consumer_kill" and injected and restart_at and time.time() >= restart_at:
            procs["proc1"] = spawn(
                "processor.py",
                processor_args(config, in_topic, out_topic, group, txn_id, workdir, "proc1"),
                workdir,
                "proc1b",
            )
            proc1 = procs["proc1"]
            restart_at = None
            record["events"].append({"event": "consumer_restarted", "t": now_ms()})

        if scenario == "broker_kill" and not injected and acked >= 10000:
            kill_at = now_ms()
            probe_thread = threading.Thread(
                target=recovery_probe, args=(kill_at, recovery), daemon=True
            )
            probe_thread.start()
            broker.kill()
            restart_at = time.time() + 5
            injected = True
            record["events"].append({"event": "broker_killed", "t": kill_at, "acked": acked})
        if scenario == "broker_kill" and injected and restart_at and time.time() >= restart_at:
            ok = broker.start()
            restart_at = None
            record["events"].append({"event": "broker_restarted", "t": now_ms(), "ready": ok})
            broker.snapshot_logs(os.path.join(workdir, "broker-after-kill.log"))

        if scenario == "rebalance_kill" and not injected and consumed >= 5000:
            if proc1.poll() is not None:
                record["injection_failed"] = True
                record["events"].append({"event": "processor_finished_before_trigger"})
                break
            proc2 = spawn(
                "processor.py",
                processor_args(config, in_topic, out_topic, group, txn_id, workdir, "proc2"),
                workdir,
                "proc2",
            )
            procs["proc2"] = proc2
            injected = True
            kill_at = time.time() + 1.0
            record["events"].append({"event": "second_processor_started", "t": now_ms(), "consumed": consumed})
        if scenario == "rebalance_kill" and injected and kill_at and time.time() >= kill_at:
            if proc1.poll() is None:
                os.kill(proc1.pid, signal.SIGKILL)
                record["events"].append({"event": "first_processor_killed", "t": now_ms()})
            else:
                record["events"].append(
                    {"event": "first_processor_already_exited", "code": proc1.poll(), "t": now_ms()}
                )
            kill_at = None

        # Completion: a processor summary says completed and the source is done.
        for tag in ("proc1", "proc2"):
            summ = read_json(os.path.join(workdir, tag + ".summary.json"))
            if summ and summ.get("status") == "completed" and source.poll() is not None:
                completed = True
        if completed:
            break
        time.sleep(0.1)

    record["t_end"] = now_ms()
    record["completed"] = completed
    if not completed and not record["injection_failed"]:
        record["status"] = "timeout_or_incomplete"
    elif record["injection_failed"]:
        record["status"] = "injection_failed"
    else:
        record["status"] = "completed"

    # Stop anything still running.
    for proc in [source, proc1, proc2]:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=30)

    record["source_summary"] = read_json(os.path.join(workdir, "source.summary.json"))
    record["proc1_summary"] = read_json(os.path.join(workdir, "proc1.summary.json"))
    record["proc2_summary"] = read_json(os.path.join(workdir, "proc2.summary.json"))
    record["proc_exit_codes"] = {
        "source": source.poll(),
        "proc1": proc1.poll() if proc1 else None,
        "proc2": proc2.poll() if proc2 else None,
    }
    if recovery:
        record["broker_recovery"] = recovery

    # First post-kill acknowledgement delay from the source ack log.
    if scenario == "broker_kill" and kill_at:
        try:
            last_pre = None
            first_post = None
            with open(os.path.join(workdir, "ack_log.jsonl")) as fh:
                for line in fh:
                    rec = json.loads(line)
                    if rec["err"] is None:
                        if rec["t_ack"] <= kill_at:
                            last_pre = rec["t_ack"]
                        elif first_post is None:
                            first_post = rec["t_ack"]
            if last_pre is not None and first_post is not None:
                record["ack_gap_ms"] = first_post - last_pre
        except Exception:
            pass

    if completed:
        ver = subprocess.run(
            [
                sys.executable, os.path.join(REPO, "src", "verifier.py"),
                "--bootstrap", BOOTSTRAP,
                "--topic", out_topic,
                "--group", f"ksl-verify-{runid}",
                "--ack-log", os.path.join(workdir, "ack_log.jsonl"),
                "--n", str(N),
                "--out", os.path.join(workdir, "verifier.json"),
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
            timeout=400,
            env=CHILD_ENV,
        )
        record["verifier"] = read_json(os.path.join(workdir, "verifier.json"))
        record["verifier_exit"] = ver.returncode
        if ver.returncode != 0:
            record["status"] = "verifier_failed"
            record["verifier_stderr_tail"] = ver.stderr[-2000:]

    out_path = os.path.join(REPO, "results", "runs", runid + ".json")
    write_json_atomic(out_path, record)
    delete_topics([in_topic, out_topic])
    print(f"{runid}: status={record['status']} completed={completed}", flush=True)
    return record


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--configs", nargs="+", default=["A", "B"])
    ap.add_argument(
        "--scenarios",
        nargs="+",
        default=["baseline", "consumer_kill", "broker_kill", "rebalance_kill"],
    )
    ap.add_argument("--reps", type=int, default=3)
    args = ap.parse_args()

    os.makedirs(RUNS_DIR, exist_ok=True)
    os.makedirs(os.path.join(REPO, "results", "runs"), exist_ok=True)

    broker = Broker()
    if not broker.fresh_cluster():
        raise SystemExit("broker did not become ready")

    try:
        for config in args.configs:
            for scenario in args.scenarios:
                for rep in range(1, args.reps + 1):
                    rec = run_one(broker, config, scenario, rep)
                    if rec["injection_failed"] and scenario != "baseline":
                        rec2 = run_one(broker, config, scenario, rep, suffix="x")
                        print(f"replacement for {rec['runid']}: {rec2['status']}", flush=True)
                    time.sleep(2)
    finally:
        broker.snapshot_logs(os.path.join(WORK_DIR, "broker-full.log"))
        broker.stop()
    print("matrix done")


if __name__ == "__main__":
    main()

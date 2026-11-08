"""Record exact component versions into results/versions.json.

Run on the machine of record while the broker container is up:
    python3 scripts/write_versions.py
The interpreter must have confluent-kafka importable.
"""

import json
import os
import platform
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def grab(cmd):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        lines = (out.stdout + out.stderr).strip().splitlines()
        return lines[0] if lines else "unavailable"
    except Exception as exc:
        return f"unavailable: {exc}"


def main():
    import confluent_kafka

    versions = {
        "machine": {
            "hostname": platform.node(),
            "kernel": platform.release(),
            "machine": platform.machine(),
        },
        "python": platform.python_version(),
        "docker": grab(["docker", "version", "--format", "{{.Server.Version}}"]),
        "kafka": "3.9.0",
        "kafka_image": "apache/kafka:3.9.0",
        "kafka_image_id": grab(
            ["docker", "image", "inspect", "--format", "{{.Id}}", "apache/kafka:3.9.0"]
        ),
        "kafka_topics_version_line": grab(
            ["docker", "exec", "ksl-kafka", "/opt/kafka/bin/kafka-topics.sh", "--version"]
        ),
        "confluent_kafka": confluent_kafka.version(),
        "librdkafka": confluent_kafka.libversion()[0],
    }
    out = os.path.join(REPO, "results", "versions.json")
    with open(out, "w") as fh:
        json.dump(versions, fh, indent=1, sort_keys=True)
    print(json.dumps(versions, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Portfolio standard checks: the lock holds, required files exist, no public grade."""
import hashlib
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREREG = os.path.join(ROOT, "PREREGISTRATION.md")
# SHA-256 of PREREGISTRATION.md as committed in 027ef11 (tag proposal-v1), before any code or run.
LOCKED_SHA256 = "e064f94b60e864102ce8d0b399b04729b01db186e10a1c41f314fe409e971072"
LOCKED_BYTES = 11457
# SHA-256 of the whole file after Amendment A1 (acc54de), appended before any measured run.
AMENDED_SHA256 = "17be0e7518ecf63ee667528a14129eaeed2477a67689f2df5345a72c92c3fb14"


def _prereg():
    with open(PREREG, "rb") as f:
        return f.read()


def test_locked_text_still_opens_the_preregistration():
    data = _prereg()
    assert hashlib.sha256(data[:LOCKED_BYTES]).hexdigest() == LOCKED_SHA256, \
        "the locked text of PREREGISTRATION.md (027ef11) changed; amendments may only be appended"


def test_preregistration_is_unchanged_since_amendment_a1():
    assert hashlib.sha256(_prereg()).hexdigest() == AMENDED_SHA256, \
        "PREREGISTRATION.md changed after Amendment A1; record a new amendment and update this pin in review"


def test_required_files_exist():
    need = ["README.md", "PREREGISTRATION.md", "AUDIT.md", "RESULTS.md", "DECISIONS.md", "CHANGELOG.md",
            "CITATION.cff", "SECURITY.md", "LICENSE", "results/summary.json", "results/ack_gaps.json",
            "results/versions.json"]
    missing = [p for p in need if not os.path.exists(os.path.join(ROOT, p))]
    assert not missing, "missing: " + ", ".join(missing)


def test_every_registered_run_has_a_record():
    runs = os.listdir(os.path.join(ROOT, "results", "runs"))
    assert len([r for r in runs if r.endswith(".json")]) == 24


def test_no_public_grade():
    for name in ("README.md", "AUDIT.md", "RESULTS.md", "CHANGELOG.md", "DECISIONS.md"):
        text = open(os.path.join(ROOT, name)).read()
        for bad in ("of 10,000", "/10000", "Grade:", "Total deducted"):
            assert bad not in text, name + " carries a grade (" + bad + "); grades are internal"

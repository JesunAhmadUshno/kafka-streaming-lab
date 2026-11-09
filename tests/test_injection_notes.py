import json, os, sys

REPO = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(REPO, "scripts"))

from make_results import injection_note


def test_late_trigger_is_printed_from_the_run_events():
    with open(os.path.join(REPO, "results", "runs", "A-consumer_kill-r2.json")) as fh:
        rec = json.load(fh)
    note = injection_note(rec)
    assert "triggered at consumed 8,900 (44.5 percent of N; locked trigger 5,000)" in note
    assert "SIGKILL delivered" in note


def test_baseline_has_no_trigger_note():
    rec = {"scenario": "baseline", "n": 20000, "events": []}
    assert injection_note(rec) == ""

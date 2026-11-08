import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from counting import count_correctness, percentile
from common import make_output_value, make_value, parse_output_value, parse_value


def test_clean_stream():
    recs = [(i, 1000 + i, 1005 + i) for i in range(100)]
    out = count_correctness(recs, set(range(100)))
    assert out["unique"] == 100
    assert out["duplicates"] == 0
    assert out["losses"] == 0
    assert out["out_of_order"] == 0
    assert out["latency_ms"]["p50"] == 5.0


def test_duplicates_and_losses():
    recs = [(0, 0, 1), (1, 0, 1), (1, 0, 2), (2, 0, 1), (2, 0, 3), (4, 0, 1)]
    out = count_correctness(recs, {0, 1, 2, 3, 4})
    assert out["unique"] == 4
    assert out["duplicates"] == 2
    assert out["losses"] == 1  # seq 3 acknowledged, never seen
    assert out["lost_seqs_sample"] == [3]


def test_out_of_order_counts_descents():
    recs = [(0, 0, 1), (2, 0, 1), (1, 0, 1), (3, 0, 1), (1, 0, 2)]
    out = count_correctness(recs, {0, 1, 2, 3})
    assert out["out_of_order"] == 2  # the 1 after 2, and the second 1
    assert out["duplicates"] == 1


def test_unacked_absent_is_not_a_loss():
    recs = [(0, 0, 1)]
    out = count_correctness(recs, {0})
    assert out["losses"] == 0


def test_percentile_interpolates():
    vals = [0.0, 10.0, 20.0, 30.0]
    assert percentile(vals, 0.5) == 15.0
    assert percentile([], 0.5) is None
    assert percentile([7.0], 0.99) == 7.0


def test_value_roundtrip_and_size():
    v = make_value(42, 123456789)
    assert len(v) == 256
    assert parse_value(v) == (42, 123456789)
    o = make_output_value(7, 100, 130)
    assert len(o) == 256
    assert parse_output_value(o) == (7, 100, 130)
    s = make_value(-1, 5)
    assert parse_value(s) == (-1, 5)

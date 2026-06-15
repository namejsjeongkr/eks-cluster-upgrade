"""Test phase timing utilities."""

import pytest

from eksupgrade.utils import PhaseTimer, _fmt_duration


def test_fmt_duration_formats():
    assert _fmt_duration(45) == "45s"
    assert _fmt_duration(0) == "0s"
    assert _fmt_duration(62) == "1m02s"
    assert _fmt_duration(3783) == "1h03m"
    assert _fmt_duration(None) == "-"


def test_phase_completed_records_duration():
    timer = PhaseTimer()
    with timer.phase("Control Plane") as rec:
        pass
    assert len(timer.records) == 1
    assert timer.records[0].status == "completed"
    assert timer.records[0].duration_s is not None
    assert timer.records[0].duration_s >= 0
    assert rec.name == "Control Plane"


def test_phase_failed_records_and_reraises():
    timer = PhaseTimer()
    with pytest.raises(ValueError):
        with timer.phase("addon: x"):
            raise ValueError("boom")
    assert timer.records[0].status == "failed"
    assert timer.records[0].duration_s is not None


def test_start_finish_manual_path():
    timer = PhaseTimer()
    rec = timer.start("nodegroup: a")
    assert rec.status == "running"
    assert len(timer.records) == 1
    timer.finish(rec)
    assert rec.status == "completed"
    assert rec.duration_s is not None


def test_summary_table_builds_with_total():
    timer = PhaseTimer()
    with timer.phase("Control Plane"):
        pass
    table = timer.summary_table()
    assert table is not None
    assert table.row_count >= 2  # 1 phase + TOTAL

"""Test the preflight read-only check module."""

from unittest.mock import MagicMock

import pytest

from eksupgrade.src.preflight import PreflightFinding, PreflightResult, _check_control_plane


def _finding(severity: str) -> PreflightFinding:
    return PreflightFinding(area="Control Plane", item="version", severity=severity, detail="x")


def test_exit_code_pass_when_all_pass() -> None:
    result = PreflightResult(findings=[_finding("pass")], check_failed=False)
    assert result.exit_code() == 0


def test_exit_code_pass_when_only_warnings() -> None:
    result = PreflightResult(findings=[_finding("warning")], check_failed=False)
    assert result.warning_count == 1
    assert result.exit_code() == 0


def test_exit_code_one_when_blocking() -> None:
    result = PreflightResult(findings=[_finding("pass"), _finding("blocking")], check_failed=False)
    assert result.blocking_count == 1
    assert result.exit_code() == 1


def test_exit_code_two_when_check_failed() -> None:
    # check_failed overrides everything, even if no blocking findings.
    result = PreflightResult(findings=[_finding("pass")], check_failed=True)
    assert result.exit_code() == 2


def test_exit_code_two_overrides_blocking() -> None:
    # check_failed (exit 2) must win even when blocking findings exist.
    result = PreflightResult(findings=[_finding("blocking")], check_failed=True)
    assert result.exit_code() == 2


def test_invalid_severity_rejected() -> None:
    with pytest.raises(ValueError):
        PreflightFinding(area="x", item="y", severity="bloking", detail="z")


def _cluster(version="1.32", target_version="1.33", status="ACTIVE"):
    c = MagicMock()
    c.version = version
    c.target_version = target_version
    c.status = status
    c.active = status == "ACTIVE"
    c.updating = status == "UPDATING"
    return c


def test_control_plane_pass_single_minor_active() -> None:
    findings = _check_control_plane(_cluster("1.32", "1.33", "ACTIVE"))
    by_item = {f.item: f.severity for f in findings}
    assert by_item["status"] == "pass"
    assert by_item["version"] == "pass"


def test_control_plane_blocking_when_updating() -> None:
    findings = _check_control_plane(_cluster("1.32", "1.33", "UPDATING"))
    assert any(f.severity == "blocking" and "UPDATING" in f.detail for f in findings)


def test_control_plane_blocking_on_multi_minor() -> None:
    findings = _check_control_plane(_cluster("1.32", "1.34", "ACTIVE"))
    assert any(f.severity == "blocking" and "minor" in f.detail.lower() for f in findings)


def test_control_plane_warns_when_already_target() -> None:
    findings = _check_control_plane(_cluster("1.33", "1.33", "ACTIVE"))
    assert any(f.severity == "warning" for f in findings)
    assert not any(f.severity == "blocking" for f in findings)


def test_control_plane_blocking_on_downgrade() -> None:
    findings = _check_control_plane(_cluster("1.33", "1.31", "ACTIVE"))
    assert any(f.severity == "blocking" and "downgrade" in f.detail.lower() for f in findings)

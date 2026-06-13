"""Test the preflight read-only check module."""

from eksupgrade.src.preflight import PreflightFinding, PreflightResult


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

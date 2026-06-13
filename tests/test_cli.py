"""Test the functionality of the CLI module."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from eksupgrade.cli import app
from eksupgrade.src.preflight import PreflightResult

runner = CliRunner()


def test_entry_version_arg() -> None:
    """Test the entry method with version argument."""
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "eksupgrade version" in result.stdout


def test_entry_no_arg() -> None:
    """Test the entry method with no arguments."""
    result = runner.invoke(app, [])
    assert result.exit_code == 2
    # Newer Click routes usage errors to stderr; result.output captures both streams.
    assert "OPTIONS" in result.output


def test_preflight_runs_check_and_exits_without_upgrade() -> None:
    fake_cluster = MagicMock()
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster) as mock_get,
        patch(
            "eksupgrade.cli.run_preflight", return_value=PreflightResult(findings=[], check_failed=False)
        ) as mock_pre,
    ):
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--no-interactive"])
    mock_get.assert_called_once()
    mock_pre.assert_called_once()
    # The cluster's mutating methods must never be called in preflight mode.
    fake_cluster.update_cluster.assert_not_called()
    fake_cluster.upgrade_addons.assert_not_called()
    assert result.exit_code == 0


def test_preflight_crash_exits_nonzero() -> None:
    fake_cluster = MagicMock()
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster),
        patch("eksupgrade.cli.run_preflight", side_effect=RuntimeError("kube down")),
    ):
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--no-interactive"])
    fake_cluster.update_cluster.assert_not_called()
    assert result.exit_code == 2

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
    fake_cluster.upgrade_nodegroups.assert_not_called()
    assert result.exit_code == 0


def test_preflight_force_still_exits_without_upgrade() -> None:
    # --preflight --force must NOT reach any mutation: the Exit dominates the
    # force flag (force is only read at the confirm/drain steps, after preflight).
    fake_cluster = MagicMock()
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster),
        patch("eksupgrade.cli.run_preflight", return_value=PreflightResult(findings=[], check_failed=False)),
    ):
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--force"])
    fake_cluster.update_cluster.assert_not_called()
    fake_cluster.upgrade_addons.assert_not_called()
    fake_cluster.upgrade_nodegroups.assert_not_called()
    assert result.exit_code == 0


def test_preflight_blocking_exits_one() -> None:
    # A blocking finding must bubble through the CLI as exit code 1.
    from eksupgrade.src.preflight import PreflightFinding

    fake_cluster = MagicMock()
    blocking = PreflightResult(
        findings=[PreflightFinding(area="Control Plane", item="version", severity="blocking", detail="x")],
        check_failed=False,
    )
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster),
        patch("eksupgrade.cli.run_preflight", return_value=blocking),
    ):
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--no-interactive"])
    fake_cluster.update_cluster.assert_not_called()
    assert result.exit_code == 1


def test_preflight_crash_exits_nonzero() -> None:
    fake_cluster = MagicMock()
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster),
        patch("eksupgrade.cli.run_preflight", side_effect=RuntimeError("kube down")),
    ):
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--no-interactive"])
    fake_cluster.update_cluster.assert_not_called()
    assert result.exit_code == 2


def test_timing_summary_printed_on_success():
    fake_cluster = MagicMock()
    fake_cluster.version = "1.34"
    fake_cluster.target_version = "1.35"
    fake_cluster.available = True
    fake_cluster.active = True
    fake_cluster.status = "ACTIVE"
    fake_cluster.upgradable_managed_nodegroups = []
    fake_cluster.nodegroups = []
    fake_cluster.nodegroup_names = []
    fake_cluster.asg_names = []
    with (
        patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster),
        patch("eksupgrade.cli.is_cluster_auto_scaler_present", return_value=(False, 0, "", "")),
        patch("eksupgrade.cli.is_karpenter_present", return_value=(False, 0, "")),
        patch("eksupgrade.cli.handle_karpenter_drift", return_value="no_drift"),
        patch("eksupgrade.cli.console.print") as mock_print,
    ):
        runner.invoke(app, ["c", "1.35", "ap-northeast-2", "--no-interactive"])
    # timing summary Table printed via console.print at least once
    assert mock_print.called

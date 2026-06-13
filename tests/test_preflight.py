"""Test the preflight read-only check module."""

from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from eksupgrade.src.preflight import (
    PreflightFinding,
    PreflightResult,
    _check_addons,
    _check_control_plane,
    _check_karpenter,
    _check_managed_nodegroups,
)


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


def _addon(name, version, target_version, available_versions):
    a = MagicMock()
    a.name = name
    a.version = version
    a.target_version = target_version
    a.available_versions = available_versions
    return a


def test_addons_pass_when_compatible_version_exists() -> None:
    cluster = MagicMock()
    cluster.addons = [_addon("coredns", "v1.11.4", "v1.12.4", ["v1.12.4", "v1.11.4"])]
    findings = _check_addons(cluster)
    assert any(f.item == "coredns" and f.severity == "pass" for f in findings)


def test_addons_blocking_when_no_compatible_version() -> None:
    cluster = MagicMock()
    cluster.addons = [_addon("coredns", "v1.11.4", "", [])]
    findings = _check_addons(cluster)
    assert any(f.item == "coredns" and f.severity == "blocking" for f in findings)


def test_addons_warning_on_lookup_failure() -> None:
    # available_versions raising simulates a describe_addon_versions failure.
    bad = MagicMock()
    bad.name = "vpc-cni"
    type(bad).available_versions = PropertyMock(side_effect=RuntimeError("boom"))
    cluster = MagicMock()
    cluster.addons = [bad]
    findings = _check_addons(cluster)
    assert any(f.item == "vpc-cni" and f.severity == "warning" for f in findings)


def _ng(name, ami_type, version="1.32"):
    n = MagicMock()
    n.name = name
    n.ami_type = ami_type
    n.version = version
    return n


def test_managed_ng_pass_non_custom() -> None:
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-al2", "AL2_x86_64")]
    findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-al2" and f.severity == "pass" for f in findings)


def test_managed_ng_custom_pass_when_ami_resolves() -> None:
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-br", "CUSTOM")]
    with patch("eksupgrade.src.preflight.get_latest_ami", return_value="ami-0abc") as mock_ami:
        findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-br" and f.severity == "pass" and "ami-0abc" in f.detail for f in findings)
    # Argument contract must mirror the working self_managed.py CUSTOM call.
    mock_ami.assert_called_once_with("1.33", "bottlerocket", "bottlerocket", "ap-northeast-2")


def test_managed_ng_custom_blocking_when_ami_resolve_fails() -> None:
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-br", "CUSTOM")]
    with patch("eksupgrade.src.preflight.get_latest_ami", side_effect=RuntimeError("no ami")):
        findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-br" and f.severity == "blocking" for f in findings)


def test_managed_ng_custom_ami_resolves_via_real_ssm_path() -> None:
    # Mock at the boto/ssm layer (not get_latest_ami) so the argument mapping is
    # actually exercised: the bottlerocket branch builds an SSM path from instance_type.
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-br", "CUSTOM")]

    fake_ssm = MagicMock()
    fake_ssm.get_parameters.return_value = {"Parameters": [{"Value": "ami-real"}]}
    fake_ec2 = MagicMock()

    def _client(service, region_name=None):
        return fake_ssm if service == "ssm" else fake_ec2

    with patch("eksupgrade.src.latest_ami.boto3.client", side_effect=_client):
        findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-br" and f.severity == "pass" and "ami-real" in f.detail for f in findings)
    called_names = fake_ssm.get_parameters.call_args.kwargs.get("Names") or fake_ssm.get_parameters.call_args.args[0]
    assert any("bottlerocket/aws-k8s-1.33" in n for n in called_names)


def test_managed_ng_custom_blocking_when_ami_unresolved() -> None:
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-br", "CUSTOM")]
    with patch("eksupgrade.src.preflight.get_latest_ami", return_value="NAN"):
        findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-br" and f.severity == "blocking" for f in findings)


def test_karpenter_skip_when_no_crd() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    cluster.region = "ap-northeast-2"
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", side_effect=Exception("not found")):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    assert not any(f.severity == "blocking" for f in findings)


def test_karpenter_pass_with_alias_nodeclass() -> None:
    cluster = MagicMock()
    nc = {"metadata": {"name": "default"}, "spec": {"amiSelectorTerms": [{"alias": "bottlerocket@latest"}]}}
    with (
        patch("eksupgrade.src.preflight.get_ec2nodeclasses", return_value=[nc]),
        patch("eksupgrade.src.preflight._list_nodepools", return_value=[{"metadata": {"name": "np"}}]),
        patch("eksupgrade.src.preflight._list_nodeclaims", return_value=[]),
    ):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    assert any("alias" in f.detail for f in findings)
    assert not any(f.severity == "blocking" for f in findings)


def test_karpenter_warns_on_orphaned_nodeclaims() -> None:
    cluster = MagicMock()
    with (
        patch("eksupgrade.src.preflight.get_ec2nodeclasses", return_value=[]),
        patch("eksupgrade.src.preflight._list_nodepools", return_value=[]),
        patch("eksupgrade.src.preflight._list_nodeclaims", return_value=[{"metadata": {"name": "nc-1"}}]),
    ):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    assert any(f.severity == "warning" and "orphan" in f.detail.lower() for f in findings)

"""Read-only preflight checks for an EKS cluster upgrade.

This module performs NO mutating AWS or Kubernetes calls. It inspects the
target cluster across four areas (control plane, addons, managed node groups,
Karpenter), renders a summary report, and returns a PreflightResult whose
exit_code() follows the kubeadm/eksup severity convention:
    0 - all checks passed (warnings allowed)
    1 - at least one blocking issue
    2 - the check itself could not run
"""

from __future__ import annotations

from dataclasses import dataclass, field

from kubernetes import client as k8s_client
from packaging.version import parse as parse_version

from eksupgrade.models.eks import _default_next_minor
from eksupgrade.src.karpenter import _list_nodeclaims, _list_nodepools, classify_ami_selector, get_ec2nodeclasses
from eksupgrade.src.latest_ami import get_latest_ami

_VALID_SEVERITIES: frozenset[str] = frozenset({"pass", "warning", "blocking"})


@dataclass
class PreflightFinding:
    """A single preflight observation."""

    area: str
    item: str
    severity: str  # "pass" | "warning" | "blocking"
    detail: str

    def __post_init__(self) -> None:
        if self.severity not in _VALID_SEVERITIES:
            raise ValueError(f"severity must be one of {sorted(_VALID_SEVERITIES)}, got {self.severity!r}")


@dataclass
class PreflightResult:
    """Aggregated preflight findings and overall outcome."""

    findings: list[PreflightFinding] = field(default_factory=list)
    check_failed: bool = False

    @property
    def blocking_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if f.severity == "warning")

    def exit_code(self) -> int:
        if self.check_failed:
            return 2
        return 1 if self.blocking_count > 0 else 0


def _check_control_plane(cluster) -> list[PreflightFinding]:
    """Check that the control plane can move one minor version, and is ACTIVE."""
    findings: list[PreflightFinding] = []
    area = "Control Plane"

    if cluster.updating:
        findings.append(
            PreflightFinding(
                area, "status", "blocking", f"Cluster is UPDATING ({cluster.status}); wait for it to finish"
            )
        )
    elif not cluster.active:
        findings.append(
            PreflightFinding(area, "status", "blocking", f"Cluster is not ACTIVE (status: {cluster.status})")
        )
    else:
        findings.append(PreflightFinding(area, "status", "pass", "Cluster is ACTIVE"))

    if cluster.version == cluster.target_version:
        findings.append(
            PreflightFinding(
                area, "version", "warning", f"Already on target version {cluster.version}; nothing to upgrade"
            )
        )
    elif parse_version(cluster.target_version) < parse_version(cluster.version):
        findings.append(
            PreflightFinding(
                area, "version", "blocking", f"Downgrade {cluster.version} -> {cluster.target_version} is not supported"
            )
        )
    elif cluster.target_version == _default_next_minor(cluster.version):
        findings.append(
            PreflightFinding(area, "version", "pass", f"{cluster.version} -> {cluster.target_version} (single minor)")
        )
    else:
        next_hop = _default_next_minor(cluster.version)
        findings.append(
            PreflightFinding(
                area,
                "version",
                "blocking",
                f"Multi-minor jump {cluster.version} -> {cluster.target_version}; EKS allows one minor at a time (next: {next_hop})",
            )
        )

    return findings


def _check_managed_nodegroups(cluster, region: str) -> list[PreflightFinding]:
    """Check each managed node group; for CUSTOM amiType, verify target AMI resolves.

    For CUSTOM the tool must resolve a new AMI itself (AWS rejects version-only
    updates), so a failed resolve is blocking. Non-CUSTOM groups are AWS-managed
    rolling upgrades and only reported as pass.
    """
    findings: list[PreflightFinding] = []
    area = "Managed NodeGroups"

    for ng in cluster.nodegroups:
        if ng.ami_type != "CUSTOM":
            findings.append(
                PreflightFinding(area, ng.name, "pass", f"amiType {ng.ami_type}; AWS-managed rolling upgrade")
            )
            continue

        try:
            # Mirror the working self_managed.py CUSTOM call: get_latest_ami keys the
            # OS family off instance_type, so both instance_type and image_to_search
            # carry the os_type hint. We assume CUSTOM == Bottlerocket here (the only
            # CUSTOM family this tool resolves); this is a documented simplification.
            ami = get_latest_ami(cluster.target_version, "bottlerocket", "bottlerocket", region)
            if not ami or ami == "NAN":
                findings.append(
                    PreflightFinding(
                        area, ng.name, "blocking", "CUSTOM (assumed Bottlerocket); target AMI did not resolve"
                    )
                )
            else:
                findings.append(
                    PreflightFinding(
                        area, ng.name, "pass", f"CUSTOM (assumed Bottlerocket); target AMI resolves to {ami}"
                    )
                )
        except Exception as exc:  # noqa: BLE001 - read-only check must not abort
            findings.append(
                PreflightFinding(
                    area, ng.name, "blocking", f"CUSTOM (assumed Bottlerocket); could not resolve target AMI: {exc}"
                )
            )

    return findings


def _check_karpenter(cluster, region: str) -> list[PreflightFinding]:
    """Inspect Karpenter NodeClasses/NodePools/NodeClaims read-only.

    Karpenter node upgrades happen via drift, not by this tool, so nothing here
    is blocking. We surface the AMI-selector style (alias auto-drifts; pinned
    does not) and warn on a broken state (orphaned NodeClaims with no NodePool).
    """
    findings: list[PreflightFinding] = []
    area = "Karpenter"

    try:
        nodeclasses = get_ec2nodeclasses(cluster.name, region)
    except Exception:  # noqa: BLE001 - CRD absence means Karpenter not in use
        return [PreflightFinding(area, "karpenter", "pass", "Karpenter not detected (skipped)")]

    custom_api = k8s_client.CustomObjectsApi()
    nodepools = _list_nodepools(custom_api)
    nodeclaims = _list_nodeclaims(custom_api)

    if not nodeclasses and not nodepools and nodeclaims:
        findings.append(
            PreflightFinding(
                area,
                "nodeclaims",
                "warning",
                f"{len(nodeclaims)} orphaned NodeClaim(s) with no NodePool/EC2NodeClass; Karpenter controller likely removed",
            )
        )
        return findings

    if not nodeclasses and not nodepools and not nodeclaims:
        return [PreflightFinding(area, "karpenter", "pass", "Karpenter not in use (no NodePools)")]

    for nc in nodeclasses:
        name = nc.get("metadata", {}).get("name", "?")
        style = classify_ami_selector(nc)
        if style == "alias":
            detail = "alias selector; nodes auto-drift on control-plane upgrade"
        else:
            detail = "pinned selector (id/name/tags); nodes will NOT auto-drift"
        findings.append(PreflightFinding(area, name, "warning" if style == "pinned" else "pass", detail))

    return findings


def _check_addons(cluster) -> list[PreflightFinding]:
    """Check each installed addon has a target-compatible version available."""
    findings: list[PreflightFinding] = []
    area = "Addons"

    for addon in cluster.addons:
        try:
            available = addon.available_versions
            target = addon.target_version
        except Exception as exc:  # noqa: BLE001 - read-only check must not abort
            findings.append(
                PreflightFinding(area, addon.name, "warning", f"Could not resolve compatible versions: {exc}")
            )
            continue

        if available:
            findings.append(PreflightFinding(area, addon.name, "pass", f"{addon.version} -> {target or '(default)'}"))
        else:
            findings.append(
                PreflightFinding(area, addon.name, "blocking", "No compatible version for target cluster version")
            )

    return findings

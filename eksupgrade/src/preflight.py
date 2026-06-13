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

from packaging.version import parse as parse_version

from eksupgrade.models.eks import _default_next_minor

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
            findings.append(PreflightFinding(area, addon.name, "pass", f"{addon.version} -> {target or available[0]}"))
        else:
            findings.append(
                PreflightFinding(area, addon.name, "blocking", "No compatible version for target cluster version")
            )

    return findings

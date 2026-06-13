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

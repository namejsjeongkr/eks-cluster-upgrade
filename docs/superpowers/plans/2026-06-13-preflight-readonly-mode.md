# `--preflight` Read-Only 점검 모드 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `--preflight`를 실제 read-only 점검 모드로 구현 — control plane/addons/managed NG/Karpenter를 조회만 하고 리치 리포트를 출력한 뒤, 업그레이드 없이 심각도별 exit code로 종료.

**Architecture:** 신규 `eksupgrade/src/preflight.py`에 `PreflightFinding`/`PreflightResult` 데이터클래스와 4개 독립 점검 함수 + `run_preflight()` 진입점을 둔다. cli.py는 `Cluster.get()` 직후 `if preflight: raise typer.Exit(run_preflight(...).exit_code())` 한 줄만 호출 — 항상 종료하여 mutation을 구조적으로 차단한다.

**Tech Stack:** Python 3.10+, typer, rich(typer 의존성), 기존 `eksupgrade.models.eks.Cluster` 및 `eksupgrade.src.karpenter`/`latest_ami` 로직 재사용. 테스트는 pytest + unittest.mock.

참고 스펙: `docs/superpowers/specs/2026-06-13-preflight-readonly-mode-design.md`

---

## 재사용하는 기존 인터페이스 (확인됨)

- `Cluster` 속성: `.version: str`, `.target_version: str`, `.status: str`, `.active: bool`(status=="ACTIVE"), `.available: bool`, `.updating: bool`, `.needs_upgrade: bool`, `.addons: list[ClusterAddon]`, `.nodegroups: list[ManagedNodeGroup]`
- `ClusterAddon`: `.name: str`, `.version: str`, `.target_version: str`, `.needs_upgrade: bool`, `.available_versions: list[str]`, `.default_version: str`
- `ManagedNodeGroup`: `.name: str`, `.version: str`, `.ami_type: str`, `.needs_upgrade: bool`
- `_default_next_minor(version: str) -> str` (eksupgrade.models.eks)
- `eksupgrade.src.latest_ami.get_latest_ami(cluster_version, instance_type, image_to_search, region) -> str`
- `eksupgrade.src.karpenter`: `get_ec2nodeclasses(cluster_name, region) -> list[dict]`, `classify_ami_selector(ec2nodeclass: dict) -> str`("alias"|"pinned"), `_list_nodepools(custom_api) -> list[dict]`, `_list_nodeclaims(custom_api) -> list[dict]`; 상수 `KARPENTER_CORE_GROUP`, `KARPENTER_AWS_GROUP`, `KARPENTER_VERSION`; `client.CustomObjectsApi()` 사용 패턴

## 파일 구조

- Create: `eksupgrade/src/preflight.py` — 데이터클래스 + 점검 함수 + 리포트 렌더 + `run_preflight()`
- Modify: `eksupgrade/cli.py:62-66` — preflight 경고 블록을 `run_preflight` 호출 + `typer.Exit`로 교체
- Create: `tests/test_preflight.py` — 단위 테스트
- Modify: `tests/test_cli.py` — preflight early-return 케이스 추가

---

### Task 1: PreflightFinding / PreflightResult 데이터클래스와 exit_code 분기

**Files:**
- Create: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_preflight.py
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'eksupgrade.src.preflight'`

- [ ] **Step 3: Write minimal implementation**

```python
# eksupgrade/src/preflight.py
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


@dataclass
class PreflightFinding:
    """A single preflight observation."""

    area: str
    item: str
    severity: str  # "pass" | "warning" | "blocking"
    detail: str


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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add PreflightResult with severity-based exit codes"
```

---

### Task 2: Control plane 점검 (`_check_control_plane`)

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_preflight.py
from unittest.mock import MagicMock

from eksupgrade.src.preflight import _check_control_plane


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
    assert any(f.severity == "pass" for f in findings)
    assert not any(f.severity == "blocking" for f in findings)


def test_control_plane_blocking_when_updating() -> None:
    findings = _check_control_plane(_cluster("1.32", "1.33", "UPDATING"))
    assert any(f.severity == "blocking" and "UPDATING" in f.detail for f in findings)


def test_control_plane_blocking_on_multi_minor() -> None:
    findings = _check_control_plane(_cluster("1.32", "1.34", "ACTIVE"))
    assert any(f.severity == "blocking" and "minor" in f.detail.lower() for f in findings)


def test_control_plane_warns_when_already_target() -> None:
    findings = _check_control_plane(_cluster("1.33", "1.33", "ACTIVE"))
    assert any(f.severity == "warning" for f in findings)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -k control_plane -v`
Expected: FAIL with `ImportError: cannot import name '_check_control_plane'`

- [ ] **Step 3: Write minimal implementation**

Add to `eksupgrade/src/preflight.py` (import `_default_next_minor` at top):

```python
from eksupgrade.models.eks import _default_next_minor  # add to imports
```

```python
def _check_control_plane(cluster) -> list[PreflightFinding]:
    """Check that the control plane can move one minor version, and is ACTIVE."""
    findings: list[PreflightFinding] = []
    area = "Control Plane"

    if cluster.updating:
        findings.append(
            PreflightFinding(area, "status", "blocking", f"Cluster is UPDATING ({cluster.status}); wait for it to finish")
        )
    elif not cluster.active:
        findings.append(
            PreflightFinding(area, "status", "blocking", f"Cluster is not ACTIVE (status: {cluster.status})")
        )
    else:
        findings.append(PreflightFinding(area, "status", "pass", "Cluster is ACTIVE"))

    if cluster.version == cluster.target_version:
        findings.append(
            PreflightFinding(area, "version", "warning", f"Already on target version {cluster.version}; nothing to upgrade")
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -k control_plane -v`
Expected: PASS (4 passed)

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add control-plane preflight check"
```

---

### Task 3: Addon 호환성 점검 (`_check_addons`)

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_preflight.py
from eksupgrade.src.preflight import _check_addons


def _addon(name, version, target_version, available_versions, needs_upgrade=True):
    a = MagicMock()
    a.name = name
    a.version = version
    a.target_version = target_version
    a.available_versions = available_versions
    a.needs_upgrade = needs_upgrade
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
    type(bad).available_versions = property(lambda self: (_ for _ in ()).throw(RuntimeError("boom")))
    cluster = MagicMock()
    cluster.addons = [bad]
    findings = _check_addons(cluster)
    assert any(f.item == "vpc-cni" and f.severity == "warning" for f in findings)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -k addons -v`
Expected: FAIL with `ImportError: cannot import name '_check_addons'`

- [ ] **Step 3: Write minimal implementation**

Add to `eksupgrade/src/preflight.py`:

```python
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
            findings.append(
                PreflightFinding(area, addon.name, "pass", f"{addon.version} -> {target or available[0]}")
            )
        else:
            findings.append(
                PreflightFinding(area, addon.name, "blocking", f"No compatible version for target cluster version")
            )

    return findings
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -k addons -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add addon-compatibility preflight check"
```

---

### Task 4: Managed NodeGroup / AMI resolve 점검 (`_check_managed_nodegroups`)

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_preflight.py
from unittest.mock import patch

from eksupgrade.src.preflight import _check_managed_nodegroups


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
    # Argument contract must mirror the working self_managed.py CUSTOM call:
    # get_latest_ami(cluster_version, instance_type=os_type, image_to_search=os_type, region).
    # get_latest_ami keys the OS family off instance_type, so it must NOT be "".
    mock_ami.assert_called_once_with("1.33", "bottlerocket", "bottlerocket", "ap-northeast-2")


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
    # Confirms the resolved SSM path is the bottlerocket x86_64 path for the target version.
    called_names = fake_ssm.get_parameters.call_args.kwargs.get("Names") or fake_ssm.get_parameters.call_args.args[0]
    assert any("bottlerocket/aws-k8s-1.33" in n for n in called_names)


def test_managed_ng_custom_blocking_when_ami_resolve_fails() -> None:
    cluster = MagicMock()
    cluster.version = "1.32"
    cluster.target_version = "1.33"
    cluster.nodegroups = [_ng("ng-br", "CUSTOM")]
    with patch("eksupgrade.src.preflight.get_latest_ami", side_effect=RuntimeError("no ami")):
        findings = _check_managed_nodegroups(cluster, region="ap-northeast-2")
    assert any(f.item == "ng-br" and f.severity == "blocking" for f in findings)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -k managed_ng -v`
Expected: FAIL with `ImportError: cannot import name '_check_managed_nodegroups'`

- [ ] **Step 3: Write minimal implementation**

Add to `eksupgrade/src/preflight.py` (import get_latest_ami at top):

```python
from eksupgrade.src.latest_ami import get_latest_ami  # add to imports
```

```python
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
            findings.append(PreflightFinding(area, ng.name, "pass", f"amiType {ng.ami_type}; AWS-managed rolling upgrade"))
            continue

        try:
            # Mirror the working self_managed.py CUSTOM call: get_latest_ami keys the
            # OS family off instance_type, so both instance_type and image_to_search
            # carry the os_type hint. We assume CUSTOM == Bottlerocket here (the only
            # CUSTOM family this tool resolves); this is a documented simplification.
            ami = get_latest_ami(cluster.target_version, "bottlerocket", "bottlerocket", region)
            findings.append(PreflightFinding(area, ng.name, "pass", f"CUSTOM; target AMI resolves to {ami}"))
        except Exception as exc:  # noqa: BLE001 - read-only check must not abort
            findings.append(
                PreflightFinding(area, ng.name, "blocking", f"CUSTOM; could not resolve target AMI: {exc}")
            )

    return findings
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -k managed_ng -v`
Expected: PASS (4 passed — includes the call_args contract test and the
boto/ssm-layer resolution test that exercise the real argument mapping)

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add managed-nodegroup AMI-resolve preflight check"
```

---

### Task 5: Karpenter 상태 점검 (`_check_karpenter`)

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_preflight.py
from eksupgrade.src.preflight import _check_karpenter


def test_karpenter_skip_when_no_crd() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    cluster.region = "ap-northeast-2"
    # get_ec2nodeclasses raising ApiException-like error => Karpenter not installed.
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", side_effect=Exception("not found")):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    # Treated as pass/skip, never blocking.
    assert not any(f.severity == "blocking" for f in findings)


def test_karpenter_pass_with_alias_nodeclass() -> None:
    cluster = MagicMock()
    nc = {"metadata": {"name": "default"}, "spec": {"amiSelectorTerms": [{"alias": "bottlerocket@latest"}]}}
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", return_value=[nc]), patch(
        "eksupgrade.src.preflight._list_nodepools", return_value=[{"metadata": {"name": "np"}}]
    ), patch("eksupgrade.src.preflight._list_nodeclaims", return_value=[]):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    assert any("alias" in f.detail for f in findings)
    assert not any(f.severity == "blocking" for f in findings)


def test_karpenter_warns_on_orphaned_nodeclaims() -> None:
    # NodeClaims exist but NodePools are gone => orphaned/broken Karpenter state.
    cluster = MagicMock()
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", return_value=[]), patch(
        "eksupgrade.src.preflight._list_nodepools", return_value=[]
    ), patch("eksupgrade.src.preflight._list_nodeclaims", return_value=[{"metadata": {"name": "nc-1"}}]):
        findings = _check_karpenter(cluster, region="ap-northeast-2")
    assert any(f.severity == "warning" and "orphan" in f.detail.lower() for f in findings)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -k karpenter -v`
Expected: FAIL with `ImportError: cannot import name '_check_karpenter'`

- [ ] **Step 3: Write minimal implementation**

Add to `eksupgrade/src/preflight.py` (imports at top):

```python
from kubernetes import client as k8s_client  # add to imports

from eksupgrade.src.karpenter import (  # add to imports
    classify_ami_selector,
    get_ec2nodeclasses,
    _list_nodeclaims,
    _list_nodepools,
)
```

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -k karpenter -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add Karpenter-state preflight check"
```

---

### Task 6: 리포트 렌더 + run_preflight 진입점

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_preflight.py
from eksupgrade.src.preflight import run_preflight


def test_run_preflight_aggregates_and_returns_result() -> None:
    cluster = _cluster("1.32", "1.33", "ACTIVE")
    cluster.name = "c"
    cluster.region = "ap-northeast-2"
    cluster.addons = [_addon("coredns", "v1.11.4", "v1.12.4", ["v1.12.4"])]
    cluster.nodegroups = [_ng("ng-al2", "AL2_x86_64")]
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", side_effect=Exception("none")):
        result = run_preflight(cluster, region="ap-northeast-2")
    assert isinstance(result, PreflightResult)
    # Single-minor active + compatible addon + non-CUSTOM NG + no Karpenter => no blocking.
    assert result.blocking_count == 0
    assert result.exit_code() == 0
    # Findings from all four areas are present.
    areas = {f.area for f in result.findings}
    assert {"Control Plane", "Addons", "Managed NodeGroups", "Karpenter"} <= areas


def test_run_preflight_blocking_bubbles_to_exit_code() -> None:
    cluster = _cluster("1.32", "1.34", "ACTIVE")  # multi-minor => blocking
    cluster.name = "c"
    cluster.region = "ap-northeast-2"
    cluster.addons = []
    cluster.nodegroups = []
    with patch("eksupgrade.src.preflight.get_ec2nodeclasses", side_effect=Exception("none")):
        result = run_preflight(cluster, region="ap-northeast-2")
    assert result.blocking_count >= 1
    assert result.exit_code() == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_preflight.py -k run_preflight -v`
Expected: FAIL with `ImportError: cannot import name 'run_preflight'`

- [ ] **Step 3: Write minimal implementation**

Add to `eksupgrade/src/preflight.py` (imports at top):

```python
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
```

```python
_SEVERITY_BADGE = {"pass": "[green]PASS[/green]", "warning": "[yellow]WARN[/yellow]", "blocking": "[red]BLOCK[/red]"}


def _render_report(cluster, result: PreflightResult) -> None:
    """Print a rich summary report (areas as tables, overall verdict at the bottom)."""
    console = Console()
    console.print(
        Panel(
            f"Cluster: [bold]{cluster.name}[/bold]   "
            f"{cluster.version} -> {cluster.target_version}   region: {cluster.region}",
            title="eksupgrade preflight (read-only)",
        )
    )

    areas = ["Control Plane", "Addons", "Managed NodeGroups", "Karpenter"]
    for area in areas:
        rows = [f for f in result.findings if f.area == area]
        if not rows:
            continue
        table = Table(title=area, show_lines=False)
        table.add_column("Item")
        table.add_column("Status")
        table.add_column("Detail")
        for f in rows:
            table.add_row(f.item, _SEVERITY_BADGE.get(f.severity, f.severity), f.detail)
        console.print(table)

    if result.blocking_count > 0:
        verdict = "[red]NOT SAFE — resolve blocking issues before upgrading[/red]"
    elif result.warning_count > 0:
        verdict = "[yellow]SAFE TO UPGRADE — review warnings[/yellow]"
    else:
        verdict = "[green]SAFE TO UPGRADE[/green]"
    console.print(f"\nBlocking: {result.blocking_count}   Warnings: {result.warning_count}   {verdict}")


def run_preflight(cluster, region: str) -> PreflightResult:
    """Run all read-only preflight checks, print a report, and return the result."""
    findings: list[PreflightFinding] = []
    findings += _check_control_plane(cluster)
    findings += _check_addons(cluster)
    findings += _check_managed_nodegroups(cluster, region)
    findings += _check_karpenter(cluster, region)

    result = PreflightResult(findings=findings, check_failed=False)
    _render_report(cluster, result)
    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_preflight.py -k run_preflight -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Run full preflight test module**

Run: `poetry run pytest tests/test_preflight.py -v`
Expected: PASS (all tasks 1-6 tests green)

- [ ] **Step 6: Commit**

```bash
git add eksupgrade/src/preflight.py tests/test_preflight.py
git commit -m "feat: add run_preflight entrypoint with rich report"
```

---

### Task 7: cli.py 연결 — preflight면 점검 후 종료

**Files:**
- Modify: `eksupgrade/cli.py` (preflight 경고 블록 `:62-66` + Cluster.get 이후)
- Test: `tests/test_cli.py`

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_cli.py
from unittest.mock import MagicMock, patch

from eksupgrade.src.preflight import PreflightResult


def test_preflight_runs_check_and_exits_without_upgrade() -> None:
    fake_cluster = MagicMock()
    with patch("eksupgrade.cli.Cluster.get", return_value=fake_cluster) as mock_get, patch(
        "eksupgrade.cli.run_preflight", return_value=PreflightResult(findings=[], check_failed=False)
    ) as mock_pre:
        result = runner.invoke(app, ["my-cluster", "1.33", "ap-northeast-2", "--preflight", "--no-interactive"])
    mock_get.assert_called_once()
    mock_pre.assert_called_once()
    # The cluster's mutating methods must never be called in preflight mode.
    fake_cluster.update_cluster.assert_not_called()
    fake_cluster.upgrade_addons.assert_not_called()
    assert result.exit_code == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_cli.py::test_preflight_runs_check_and_exits_without_upgrade -v`
Expected: FAIL — `run_preflight` not imported in cli.py (AttributeError on patch target) or `update_cluster` was called.

- [ ] **Step 3: Write minimal implementation**

In `eksupgrade/cli.py`, add the import near the other `from .` imports (top of file):

```python
from .src.preflight import run_preflight
```

Replace the existing block at `cli.py:62-66`:

```python
    if preflight:
        echo_warning(
            "--preflight is unused and will be removed in an upcoming release. "
            "Please use an EKS upgrade readiness assessment tool such as: github.com/clowdhaus/eksup"
        )
```

Then insert the preflight block immediately after the `Cluster.get(...)` call
closes (cli.py:80) and BEFORE the `echo_info("Upgrading cluster: ...")` at
cli.py:81 — so read-only mode never prints an "Upgrading…" message:

```python
        target_cluster: Cluster = Cluster.get(
            cluster_name=cluster_name, region=region, target_version=cluster_version, latest_addons=latest_addons
        )

        # Preflight is a read-only assessment: report and exit before any mutation
        # (and before announcing an upgrade). This also defuses the
        # --preflight --no-interactive trap: we always Exit here, never reaching
        # the confirm prompt or update_cluster().
        if preflight:
            preflight_result = run_preflight(target_cluster, region)
            raise typer.Exit(code=preflight_result.exit_code())

        echo_info(
            f"Upgrading cluster: {cluster_name} from version: {target_cluster.version} to {target_cluster.target_version}...",
        )
```

Concretely: delete lines 62-66 (the old `if preflight:` warning block), and
insert the `if preflight:` Exit block between the `Cluster.get(...)` call
(closes at line 80) and the `echo_info(... Upgrading cluster ...)` (line 81).

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_cli.py::test_preflight_runs_check_and_exits_without_upgrade -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/cli.py tests/test_cli.py
git commit -m "feat: wire --preflight to run_preflight and exit before upgrade"
```

---

### Task 8: help 문구 정정 + 전체 검증

**Files:**
- Modify: `eksupgrade/cli.py:43` (preflight 옵션 help 문구)

- [ ] **Step 1: Update the help string**

In `eksupgrade/cli.py:43`, the preflight option already reads:

```python
    preflight: bool = typer.Option(default=False, help="Run pre-upgrade checks without upgrade"),
```

This is now ACCURATE, so keep it. No change needed unless the help wording in the
deleted warning suggested otherwise — confirm the option help reads
"Run pre-upgrade checks without upgrade". (No-op step if already correct.)

- [ ] **Step 2: Run the full suite + lint**

Run:
```bash
poetry run pytest -q
poetry run black --check eksupgrade/ tests/
poetry run isort --check eksupgrade/ tests/
poetry run ruff check eksupgrade/ tests/
```
Expected: all tests pass (98 prior + new preflight/cli tests), lint clean.

- [ ] **Step 3: Fix any lint findings**

Run: `poetry run black eksupgrade/ tests/ && poetry run isort eksupgrade/ tests/ && poetry run ruff check --fix eksupgrade/ tests/`
Then re-run Step 2 to confirm clean.

- [ ] **Step 4: Manual smoke (optional, read-only)**

If a non-prod cluster + credentials are available:
```bash
poetry run eksupgrade <cluster> <target> <region> --preflight --no-interactive; echo "exit=$?"
```
Expected: rich report prints, no mutation, exit 0/1/2 by severity.

- [ ] **Step 5: Commit**

```bash
git add eksupgrade/cli.py
git commit -m "docs: confirm accurate --preflight help text"
```

---

## Self-Review (작성자 점검 완료)

**Spec coverage:**
- read-only 점검만 + 항상 종료 → Task 7 (`raise typer.Exit`) ✅
- 4개 영역 점검 → Task 2/3/4/5 ✅
- rich 테이블/패널 리포트 → Task 6 `_render_report` ✅
- exit code 0/1/2 → Task 1 `exit_code()` ✅
- `--preflight --no-interactive` 함정 해소 → Task 7 (Cluster.get 직후 무조건 Exit, confirm 이전) + Task 7 테스트가 `--no-interactive`로 검증 ✅
- 부분 실패=warning, 점검 자체 실패=check_failed → Task 3 addon warning, Task 5 Karpenter skip; check_failed는 run_preflight 진입 전제(Cluster.get) 실패 시 cli에서 처리 (Cluster.get 실패는 기존 try/except가 잡음) ✅
- 고아 NodeClaim 경고 → Task 5 `test_karpenter_warns_on_orphaned_nodeclaims` ✅

**Placeholder scan:** 모든 코드 스텝에 실제 코드 포함, TBD/TODO 없음 ✅

**Type consistency:** `PreflightFinding(area, item, severity, detail)`, `PreflightResult(findings, check_failed)`, `.blocking_count`/`.warning_count`/`.exit_code()`, `_check_control_plane/_check_addons/_check_managed_nodegroups/_check_karpenter`, `run_preflight(cluster, region)` — Task 전반에서 일관 ✅

**알려진 단순화:** `check_failed`는 현재 run_preflight 내부에서 항상 False로 둔다. 점검 자체 실행 불가(Cluster.get 실패)는 cli.py의 기존 try/except 경로가 담당하므로, exit 2는 그 경로에서 발생한다. 개별 영역 실패는 warning으로 흡수 — 스펙의 의도와 일치.

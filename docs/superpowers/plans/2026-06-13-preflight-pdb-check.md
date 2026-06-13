# PDB 미커버 경고 점검 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** preflight에 5번째 점검 `_check_pod_disruption_budgets`를 추가 — replicas≥2인데 PDB로 커버되지 않는 Deployment/StatefulSet을 warning으로 보고한다.

**Architecture:** preflight.py에 read-only 점검 함수를 추가하고 `run_preflight`에 한 줄 연결한다. PDB selector(matchLabels)와 워크로드 pod 템플릿 라벨의 부분집합 매칭으로 커버 여부를 판정한다. 모든 호출은 list (read-only)이며 전체를 try/except로 감싸 조회 실패 시 warning 1개로 degrade한다.

**Tech Stack:** Python, kubernetes client (`AppsV1Api`, `PolicyV1Api`), pytest + unittest.mock.

참고 스펙: `docs/superpowers/specs/2026-06-13-preflight-readonly-mode-design.md` (Pod Disruption Budget 섹션)

---

## 확인된 인터페이스

- `kubernetes.client.AppsV1Api().list_deployment_for_all_namespaces()` / `.list_stateful_set_for_all_namespaces()` → `.items`, 각 item: `.metadata.namespace`, `.metadata.name`, `.spec.replicas` (int|None), `.spec.template.metadata.labels` (dict|None), `.kind`는 list 응답에서 비어 있을 수 있으므로 워크로드 종류는 호출 함수가 명시적으로 부여한다.
- `kubernetes.client.PolicyV1Api().list_pod_disruption_budget_for_all_namespaces()` → `.items`, 각 item: `.metadata.namespace`, `.spec.selector` (V1LabelSelector|None) with `.match_labels` (dict|None) and `.match_expressions` (list|None).
- 기존 재사용: `from eksupgrade.src.k8s_client import loading_config`; `from kubernetes import client as k8s_client` (이미 preflight.py에 있음).
- `PreflightFinding(area, item, severity, detail)` / severity ∈ {"pass","warning","blocking"}.

## 파일 구조

- Modify: `eksupgrade/src/preflight.py` — `_check_pod_disruption_budgets` 추가 + `run_preflight`에 연결
- Modify: `tests/test_preflight.py` — 단위 테스트 추가

---

### Task 1: _check_pod_disruption_budgets 구현 + run_preflight 연결

**Files:**
- Modify: `eksupgrade/src/preflight.py`
- Test: `tests/test_preflight.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_preflight.py` (imports `_check_pod_disruption_budgets`; `MagicMock`/`patch` already imported):

```python
def _workload(namespace, name, replicas, template_labels):
    w = MagicMock()
    w.metadata.namespace = namespace
    w.metadata.name = name
    w.spec.replicas = replicas
    w.spec.template.metadata.labels = template_labels
    return w


def _pdb(namespace, match_labels, match_expressions=None):
    p = MagicMock()
    p.metadata.namespace = namespace
    p.spec.selector.match_labels = match_labels
    p.spec.selector.match_expressions = match_expressions
    return p


def _patch_pdb_listers(deployments, statefulsets, pdbs):
    """Patch the three list APIs + loading_config used by _check_pod_disruption_budgets."""
    deploy_resp = MagicMock()
    deploy_resp.items = deployments
    sts_resp = MagicMock()
    sts_resp.items = statefulsets
    pdb_resp = MagicMock()
    pdb_resp.items = pdbs

    apps = MagicMock()
    apps.list_deployment_for_all_namespaces.return_value = deploy_resp
    apps.list_stateful_set_for_all_namespaces.return_value = sts_resp
    policy = MagicMock()
    policy.list_pod_disruption_budget_for_all_namespaces.return_value = pdb_resp

    return apps, policy


def test_pdb_warns_when_multireplica_uncovered() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    apps, policy = _patch_pdb_listers(
        deployments=[_workload("app", "web", 3, {"app": "web"})],
        statefulsets=[],
        pdbs=[],  # no PDB at all
    )
    with patch("eksupgrade.src.preflight.loading_config"), patch(
        "eksupgrade.src.preflight.k8s_client.AppsV1Api", return_value=apps
    ), patch("eksupgrade.src.preflight.k8s_client.PolicyV1Api", return_value=policy):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    assert any(f.item == "app/web" and f.severity == "warning" for f in findings)


def test_pdb_no_warning_when_covered() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    apps, policy = _patch_pdb_listers(
        deployments=[_workload("app", "web", 3, {"app": "web", "tier": "frontend"})],
        statefulsets=[],
        pdbs=[_pdb("app", {"app": "web"})],  # subset match covers it
    )
    with patch("eksupgrade.src.preflight.loading_config"), patch(
        "eksupgrade.src.preflight.k8s_client.AppsV1Api", return_value=apps
    ), patch("eksupgrade.src.preflight.k8s_client.PolicyV1Api", return_value=policy):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    assert not any(f.severity == "warning" for f in findings)


def test_pdb_skips_single_replica() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    apps, policy = _patch_pdb_listers(
        deployments=[_workload("app", "solo", 1, {"app": "solo"})],
        statefulsets=[],
        pdbs=[],
    )
    with patch("eksupgrade.src.preflight.loading_config"), patch(
        "eksupgrade.src.preflight.k8s_client.AppsV1Api", return_value=apps
    ), patch("eksupgrade.src.preflight.k8s_client.PolicyV1Api", return_value=policy):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    # single-replica workload is never warned about
    assert not any(f.item == "app/solo" for f in findings)


def test_pdb_wrong_namespace_does_not_cover() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    apps, policy = _patch_pdb_listers(
        deployments=[_workload("app", "web", 2, {"app": "web"})],
        statefulsets=[],
        pdbs=[_pdb("other", {"app": "web"})],  # right labels, wrong namespace
    )
    with patch("eksupgrade.src.preflight.loading_config"), patch(
        "eksupgrade.src.preflight.k8s_client.AppsV1Api", return_value=apps
    ), patch("eksupgrade.src.preflight.k8s_client.PolicyV1Api", return_value=policy):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    assert any(f.item == "app/web" and f.severity == "warning" for f in findings)


def test_pdb_statefulset_covered_and_uncovered() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    apps, policy = _patch_pdb_listers(
        deployments=[],
        statefulsets=[
            _workload("db", "covered", 3, {"app": "covered"}),
            _workload("db", "bare", 2, {"app": "bare"}),
        ],
        pdbs=[_pdb("db", {"app": "covered"})],
    )
    with patch("eksupgrade.src.preflight.loading_config"), patch(
        "eksupgrade.src.preflight.k8s_client.AppsV1Api", return_value=apps
    ), patch("eksupgrade.src.preflight.k8s_client.PolicyV1Api", return_value=policy):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    items = {f.item: f.severity for f in findings if f.severity == "warning"}
    assert items.get("db/bare") == "warning"
    assert "db/covered" not in items


def test_pdb_warns_on_lookup_failure() -> None:
    cluster = MagicMock()
    cluster.name = "c"
    with patch("eksupgrade.src.preflight.loading_config", side_effect=RuntimeError("api down")):
        findings = _check_pod_disruption_budgets(cluster, region="ap-northeast-2")
    assert any(f.severity == "warning" and "could not" in f.detail.lower() for f in findings)
    assert not any(f.severity == "blocking" for f in findings)
```

- [ ] **Step 2: Run to verify failure**

Run: `poetry run pytest tests/test_preflight.py -k pdb -v`
Expected: FAIL (ImportError: cannot import name '_check_pod_disruption_budgets')

- [ ] **Step 3: Implement in `eksupgrade/src/preflight.py`**

`_check_karpenter` 아래에 추가 (이미 import된 `k8s_client`, `loading_config` 재사용):

```python
def _pdb_covers(pdb_match_labels: dict, workload_labels: dict) -> bool:
    """True if every PDB selector label is present (subset match) in the workload labels."""
    if not pdb_match_labels:
        return False
    return all(workload_labels.get(k) == v for k, v in pdb_match_labels.items())


def _check_pod_disruption_budgets(cluster, region: str) -> list[PreflightFinding]:
    """Warn about replicas>=2 workloads not covered by any PodDisruptionBudget.

    During an upgrade these workloads are drained without an availability floor.
    Read-only: lists Deployments/StatefulSets and PDBs. Never blocking.
    """
    area = "Pod Disruption Budgets"
    try:
        loading_config(cluster.name, region)
        apps = k8s_client.AppsV1Api()
        policy = k8s_client.PolicyV1Api()
        deployments = apps.list_deployment_for_all_namespaces().items
        statefulsets = apps.list_stateful_set_for_all_namespaces().items
        pdbs = policy.list_pod_disruption_budget_for_all_namespaces().items
    except Exception as exc:  # noqa: BLE001 - read-only check must not abort
        return [PreflightFinding(area, "pdb", "warning", f"Could not list workloads/PDBs: {exc}")]

    # Group PDB selectors by namespace for subset matching.
    pdbs_by_ns: dict[str, list[dict]] = {}
    for pdb in pdbs:
        ns = pdb.metadata.namespace
        match_labels = (pdb.spec.selector.match_labels if pdb.spec and pdb.spec.selector else None) or {}
        pdbs_by_ns.setdefault(ns, []).append(match_labels)

    findings: list[PreflightFinding] = []
    for kind, workloads in (("Deployment", deployments), ("StatefulSet", statefulsets)):
        for wl in workloads:
            replicas = wl.spec.replicas or 0
            if replicas < 2:
                continue
            ns = wl.metadata.namespace
            labels = (wl.spec.template.metadata.labels if wl.spec.template and wl.spec.template.metadata else None) or {}
            covered = any(_pdb_covers(ml, labels) for ml in pdbs_by_ns.get(ns, []))
            if not covered:
                findings.append(
                    PreflightFinding(
                        area,
                        f"{ns}/{wl.metadata.name}",
                        "warning",
                        f"{kind}, replicas={replicas}, no PDB covers it",
                    )
                )

    if not findings:
        findings.append(PreflightFinding(area, "pdb", "pass", "All multi-replica workloads are covered by a PDB"))
    return findings
```

- [ ] **Step 4: Wire into run_preflight**

In `run_preflight`, add the call after `_check_karpenter`:
```python
    findings += _check_karpenter(cluster, region)
    findings += _check_pod_disruption_budgets(cluster, region)
```

- [ ] **Step 5: Run to verify pass**

Run: `poetry run pytest tests/test_preflight.py -k pdb -v` (expect 6 passed)
Then full module: `poetry run pytest tests/test_preflight.py -v` (expect 134 passed)
Then full suite: `poetry run pytest -q` (expect all green, no regressions)

- [ ] **Step 6: Lint + commit**

```bash
poetry run black eksupgrade/src/preflight.py tests/test_preflight.py
poetry run isort eksupgrade/src/preflight.py tests/test_preflight.py
poetry run ruff check eksupgrade/src/preflight.py tests/test_preflight.py
git add eksupgrade/src/preflight.py tests/test_preflight.py docs/superpowers/specs/2026-06-13-preflight-readonly-mode-design.md
git commit -m "feat: warn on multi-replica workloads without a PodDisruptionBudget"
```

---

## Self-Review

**Spec coverage:** replicas≥2 대상 ✅; PDB selector matchLabels 부분집합 매칭 ✅; warning severity ✅; read-only list API + try/except degrade ✅; namespace 격리 매칭 ✅; matchExpressions는 match_labels만 사용하므로 expressions-only PDB는 빈 dict→커버 안 함(보수적, 오탐 대신 미탐 방향이지만 warning이라 안전).

**Placeholder scan:** 모든 스텝에 실제 코드 포함, TBD 없음.

**Type consistency:** `_check_pod_disruption_budgets(cluster, region)`, `_pdb_covers(pdb_match_labels, workload_labels)`, `PreflightFinding(area,item,severity,detail)` — 일관.

**알려진 단순화:** matchExpressions만 가진 PDB는 match_labels가 비어 `_pdb_covers`가 False를 반환 → 해당 expressions로 커버되는 워크로드를 "미커버"로 경고할 수 있음(false positive 가능). warning 수준이고 운영자가 확인 가능하므로 수용. 추후 필요 시 matchExpressions 매칭 추가.

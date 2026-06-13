# `--preflight` Read-Only 점검 모드 설계

- 날짜: 2026-06-13
- 대상 저장소: eksupgrade (fork)
- 관련 파일: `eksupgrade/cli.py`, 신규 `eksupgrade/src/preflight.py`

## 배경 / 문제

현재 `--preflight` 플래그는 help 문구에 "Run pre-upgrade checks without upgrade"라고
표기되어 있으나, 실제 코드(`cli.py`)는 경고 메시지만 출력하고 **early-return 없이 실제
업그레이드 로직으로 그대로 진입**한다. 즉 help 텍스트가 거짓이며, 이 도구에는 dry-run /
read-only 모드가 사실상 존재하지 않는다.

특히 `--preflight --no-interactive` 조합은 확인 프롬프트(유일한 안전 경계)도 건너뛰고
preflight도 멈추지 않으므로 **즉시 프로덕션 업그레이드**가 실행되는 함정이 된다.

## 목표

`--preflight`를 "점검만 하고 종료"하는 진짜 read-only 모드로 구현한다.

- 어떤 mutation(AWS/k8s 쓰기)도 수행하지 않는다.
- 업그레이드 가능 여부를 4개 영역에서 점검하고 리치 테이블/패널로 요약 리포트를 출력한다.
- 점검 후 **항상 종료**한다 (업그레이드 로직으로 진입하지 않음).
- 심각도(severity)에 따라 명확한 exit code를 반환하여 CI 게이트로 활용 가능하게 한다.

## 비목표 (YAGNI)

- 실제 deprecated API 스캔, 리소스 용량 점검 등 고급 점검은 범위 밖 (추후 확장 여지).
- `--disable-checks` 플래그 정리는 별도 작업.
- 외부 도구(eksup 등)와의 통합은 하지 않는다.

## 아키텍처

### 신규 모듈: `eksupgrade/src/preflight.py`

```
@dataclass
class PreflightFinding:
    area: str           # "Control Plane" | "Addons" | "Managed NodeGroups" | "Karpenter"
    item: str           # 점검 대상 (예: addon 이름, NG 이름)
    severity: str       # "pass" | "warning" | "blocking"
    detail: str         # 사람이 읽을 설명 (현재→목표 등)

@dataclass
class PreflightResult:
    findings: list[PreflightFinding]
    check_failed: bool          # 점검 자체가 실패했는지 (권한/네트워크 등)

    @property
    def blocking_count(self) -> int: ...
    @property
    def warning_count(self) -> int: ...

    def exit_code(self) -> int:
        # 2 = 점검 실행 실패, 1 = 차단 이슈, 0 = 통과(경고만 포함)
        if self.check_failed:
            return 2
        return 1 if self.blocking_count > 0 else 0


def run_preflight(cluster: Cluster, region: str) -> PreflightResult:
    """4개 영역을 read-only로 점검하고 리포트를 출력한 뒤 결과를 반환."""
```

각 점검은 독립 함수로 분리 (단위 테스트 + 가독성):

- `_check_control_plane(cluster) -> list[PreflightFinding]`
- `_check_addons(cluster) -> list[PreflightFinding]`
- `_check_managed_nodegroups(cluster, region) -> list[PreflightFinding]`
- `_check_karpenter(cluster, region) -> list[PreflightFinding]`

리포트 출력은 `_render_report(result)` 가 담당 (rich 테이블/패널).

### cli.py 수정

`if preflight:` 블록을 다음으로 교체한다. 위치는 `Cluster.get()`(read-only) 직후,
`interactive confirm` / `update_cluster()` **이전**.

```python
target_cluster = Cluster.get(...)   # 기존, read-only

if preflight:
    result = run_preflight(target_cluster, region)
    raise typer.Exit(code=result.exit_code())   # 항상 종료 → mutation 없음
```

이로써 `--preflight --no-interactive` 함정은 구조적으로 무력화된다 (preflight면 무조건
Exit).

## 점검 항목 (모두 read-only, 기존 로직 재사용)

| # | 영역 | 재사용 로직 | blocking 조건 | warning 조건 |
|---|------|-------------|---------------|--------------|
| 1 | Control Plane | `cluster.active`, `cluster.status`, `cluster.needs_upgrade`, 멀티-마이너 가드(`_default_next_minor`) | 클러스터 비활성/UPDATING, 멀티-마이너 점프 | 이미 목표 버전 |
| 2 | Addons | addon `needs_upgrade`, `available_versions`(describe_addon_versions) | 목표 버전 호환 addon 버전 없음 | (없음) |
| 3 | Managed NodeGroups | nodegroup `amiType`, `needs_upgrade`; CUSTOM이면 목표 AMI resolve 시도(`get_latest_ami`, describe만) | CUSTOM인데 목표 AMI resolve 실패 | (없음) |
| 4 | Karpenter | `get_ec2nodeclasses`, `classify_ami_selector`, `_list_nodepools`, `_list_nodeclaims` | (없음 — Karpenter는 skew-safe로 skip 가능) | NodePool 부재인데 NodeClaim 잔존(고아), id-pinned로 drift 안 됨 |

## 데이터 흐름

```
cli.main(preflight=True)
  └─ Cluster.get()                # describe_cluster (read-only)
  └─ run_preflight(cluster, region)
       ├─ _check_control_plane    # describe/속성 조회
       ├─ _check_addons           # describe_addon / describe_addon_versions
       ├─ _check_managed_nodegroups  # describe_nodegroup, get_latest_ami(describe_images/ssm)
       ├─ _check_karpenter        # k8s list nodepools/ec2nodeclasses/nodeclaims
       └─ _render_report          # rich 테이블/패널 출력
  └─ raise typer.Exit(code)       # 0=통과/경고, 1=차단, 2=점검실패
```

## 에러 처리

- 개별 점검 함수는 **예외를 밖으로 던지지 않는다.** 한 영역의 read-only 조회 실패(권한/
  네트워크/CRD 부재 등)는 해당 항목을 `severity="warning"`(점검 불가)으로 기록하고 계속
  진행한다 (한 영역 실패가 전체 점검을 막지 않음). 이런 부분 실패는 `check_failed`를
  세팅하지 **않는다** → warning만 있는 상황이 exit 2로 잘못 격상되지 않음.
- `check_failed=True`는 **점검 자체를 수행할 수 없는 경우**로 한정한다. 즉
  `run_preflight` 진입 전제(예: `Cluster.get`의 describe_cluster)가 실패하여 점검 대상을
  확보하지 못한 상황. 이때만 exit 2.
- Karpenter CRD 미설치 클러스터는 정상 케이스로 처리 (Karpenter 미사용 = pass/skip,
  warning 아님).

## 출력 형식 (rich)

- 상단: `Panel` 헤더 — 클러스터명, 현재→목표 버전, 리전.
- 영역별 `Table` — 컬럼: `Item`, `Status`(✅ pass / ⚠️ warning / ❌ blocking), `Detail`.
- 하단: 요약 라인 —
  `Blocking: N  Warnings: M` + 판정 배지
  (`❌ NOT SAFE — resolve blocking issues` / `✅ SAFE TO UPGRADE` / `⚠️ SAFE with warnings`).

## 테스트 (`tests/test_preflight.py`)

- `PreflightResult.exit_code()` 의 3개 분기(0/1/2) 단위 테스트.
- 각 `_check_*` 함수: moto/mock 으로 pass/warning/blocking 케이스.
  - control plane: 정상 / UPDATING(blocking) / 멀티-마이너(blocking) / 이미 목표(warning)
  - addons: 호환 버전 있음 / 없음(blocking)
  - managed NG: CUSTOM AMI resolve 성공 / 실패(blocking)
  - karpenter: NodePool 있음(pass) / 고아 NodeClaim(warning) / CRD 부재(skip)
- cli: `--preflight` 시 `run_preflight` 호출 후 **업그레이드 로직 미진입**(update_cluster
  미호출) + 적절한 exit code 검증.

## exit code 체계 (확정)

| 상황 | exit code |
|------|-----------|
| 모든 점검 통과 (또는 warning만) | 0 |
| 차단(blocking) 이슈 1개 이상 | 1 |
| 점검 자체 실행 실패 (권한/네트워크 등) | 2 |

kubeadm/eksup 류의 "심각도 분리" 관례를 따른다. terraform의 `2=변경있음`은 채택하지
않는다 (preflight의 신호는 "변경 유무"가 아니라 "안전 여부"이므로).

## 후속 (이 설계 범위 밖, 별도 커밋 권장)

- `--preflight` 외에 오해를 부르는 help 문구는 본 구현으로 정정됨.
- `--disable-checks` no-op 정리.

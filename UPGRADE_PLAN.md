# eks-cluster-upgrade 고도화 계획

> 최신 EKS 지원 버전(2026 기준 1.36) 기준 현대화 + Cluster Autoscaler 노드와 Karpenter 관리 노드를 **둘 다** 안전하게 업그레이드.

## 사용자 결정 (2026-06-07)

1. **CA 복원 + 둘 다 지원** — 미커밋 diff가 삭제한 Cluster Autoscaler 로직을 되살리고, CA 노드는 기존 방식, Karpenter 노드는 drift 방식으로 분리 처리.
2. **AMI 케이스별 처리** — EC2NodeClass를 감지해서: floating alias(`al2023@latest`)면 관찰만, pinned(`al2023@v2025...`)면 사용자 승인 후에만 amiSelectorTerms 핀 갱신.

## 핵심 배경 (분석으로 확인된 사실)

- **현재 Karpenter 업그레이드 코드는 근본적으로 잘못됨**: `컨트롤러 정지(replicas=0) → ec2.terminate_instances() → 재기동`. 용량 공백, PDB/disruption budget 무시, orphan NodeClaim, AMI 미갱신.
- **올바른 방식 = Karpenter Drift**: 컨트롤러 켜둔 채 → 컨트롤플레인 업그레이드 → EC2NodeClass amiSelectorTerms 재해석 → NodeClaim에 `Drifted` 조건 → 용량 우선 + Eviction(PDB 준수) 교체.
- **EKS 최신 = 1.36** (표준 지원 1.33~1.36). 1.34 → 1.36은 single-minor-jump 규칙상 **두 번**(1.34→1.35→1.36).
- **Karpenter v1 GA**: NodePool/NodeClaim=`karpenter.sh/v1`, EC2NodeClass=`karpenter.k8s.aws/v1`. EKS 1.34는 Karpenter v1.6+.
- **EKS 1.34부터 AL2 불가** → `al2023`.
- 보존할 것(정상 동작): 컨트롤플레인 업그레이드, addon 라이브 버전 해석(`describe_addon_versions`), vpc-cni step 업그레이드, managed nodegroup AMI 롤아웃(AWS 위임).

---

## A. 사용자 요청 핵심 (필수)

### A1. Karpenter 노드 업그레이드를 drift 방식으로 재작성 (최우선)

**대상 파일**: `eksupgrade/src/k8s_client.py`, `eksupgrade/cli.py`, (신규) `eksupgrade/src/karpenter.py`

> ⚠️ **타이밍 핵심**: drift는 별도 후행 단계가 **아니다**. 컨트롤러를 켜둔 채 컨트롤플레인을 올리면, Karpenter가 즉시 새 K8s 버전 기준으로 AMI를 재해석해 **컨트롤플레인 업그레이드 직후부터** NodeClaim 교체를 시작한다(다른 nodegroup 작업과 동시 진행). 따라서 도구의 역할은 "Karpenter 노드를 종료시키는 단계"가 아니라 **(필요 시) 컨트롤플레인 직후 핀을 갱신하고, drift가 안전히 수렴할 때까지 관찰·게이트**하는 것이다.

- `upgrade_karpenter_nodes()`의 terminate 로직 **제거**.
- `cli.py`에서 Karpenter **pause/resume 제거** — 컨트롤러는 계속 켜둠.
- 신규 함수(CustomObjectsApi 사용, `karpenter.k8s.aws/v1` EC2NodeClass / `karpenter.sh/v1` NodePool·NodeClaim):
  - `get_ec2nodeclasses()` — 모든 EC2NodeClass와 각 `spec.amiSelectorTerms` 읽기.
  - `classify_ami_selector(ec2nodeclass)` — `floating`(`al2023@latest` 등 버전 추종 alias) / `pinned`(`@v2025...`) / `ami-id`(고정 id) / `name`(와일드카드) / `tags` 판별. **미인식 selector는 floating으로 가정하지 않고 pinned로 취급**(명시적 처리 요구 → drift가 자동 발생한다고 단정하지 않음).
  - `ensure_drift_target(...)`:
    - floating → 변경 없음. drift는 **A3의 컨트롤플레인 업그레이드 시점에 이미 시작**된다.
    - pinned/ami-id/미인식 → **컨트롤플레인 업그레이드 직후**, **사용자 승인 후에만** 타깃 버전 AMI로 amiSelectorTerms 갱신(끝단이 아니라 앞단에서 핀 갱신해 긴 노드 교체를 직렬화하지 않음).
  - `wait_for_karpenter_drift(...)` — NodeClaim `status.conditions[type==Drifted]`와 `status.imageID`가 신규 AMI가 될 때까지(타임아웃) 폴링. **이 게이트는 흐름 끝단**에 둔다(일찍 시작된 drift의 수렴을 기다림).
- disruption.budgets / `karpenter.sh/do-not-disrupt`를 읽어 **차단/지연되면 경고**(또는 `--force-karpenter` 동작 정의, A4 참조).
- **구현 시 경험적 검증 필요**: "floating(`@latest`)은 컨트롤플레인 업그레이드만으로 기존 노드를 drift시킨다"가 실제 클러스터의 Karpenter 버전/설정에서 성립하는지 확인 후 "floating=관찰만" 분기를 신뢰.

### A2. Cluster Autoscaler 지원 복원 + 두 경로 공존

**대상 파일**: `eksupgrade/src/k8s_client.py`, `eksupgrade/src/boto_aws.py`, `eksupgrade/cli.py`

- `git show HEAD`에서 복원: `is_cluster_auto_scaler_present()`, `cluster_auto_enable_disable()` (k8s_client), `check_asg_autoscaler()`, `enable_disable_autoscaler()` (boto_aws), `sort_pods`의 cluster-autoscaler 라벨 분기.
- `cli.py`에서 런타임에 **둘 다 감지**:
  - CA 발견 → CA 노드/self-managed ASG에 기존 pause→roll→resume 플레이북(+ `finally` 복원).
  - Karpenter 발견 → A1의 drift 플레이북(pause/terminate 아님).
  - 둘 다 있으면 둘 다, 없으면 managed nodegroup만.
- CA 의미를 Karpenter에, Karpenter 의미를 CA에 **섞지 않음**.

### A3. 최신 EKS 버전 지원 + multi-hop 처리

**대상 파일**: `eksupgrade/models/eks.py`, `eksupgrade/utils.py`, `eksupgrade/cli.py`

- `eks.py:664` `str(float(version)+0.01)` → `packaging.version` 기반 minor 증가로 교체(`1.29`→`1.30`).
- 지원 버전 검증: `aws eks describe-cluster-versions`(가능 시) 또는 유지보수 리스트로 floor/ceiling 확인(version_dict.json 삭제분 대체).
- **multi-hop**: 1.34→1.36처럼 2단계 이상이면 — 기본은 "한 번에 한 minor만, 두 번 실행" 안내, 옵션으로 `--multi-hop` 시 순차 자동 오케스트레이션(컨트롤플레인→addon→노드를 hop마다 반복). 단계별 동작은 구현 전 재확인.

### A4. Karpenter drift 안전 게이트 + 테스트

- (컨트롤플레인 업그레이드/핀 갱신으로) **이미 시작된** drift가 수렴하도록, 모든 타깃 NodeClaim이 신규 imageID + 새 Node Ready가 될 때까지 흐름 끝단에서 타임아웃 대기, 실패 시 명확히 중단.
- disruption budget이 0(업무시간 차단 등)이거나 do-not-disrupt면: 대기/경고/`--force` 정책 정의(사용자 확인).
- 단위 테스트(가짜 k8s client + moto): `is_karpenter_present`, EC2NodeClass 분류, drift 감지, Karpenter 없음 경로. `validate.yaml` CI 연동.

---

## B. 함께 발견됨 — 권장(선택)

> 사용자 요청의 핵심은 아니지만 분석에서 드러난 실제 문제. 동의 시 포함.

### B1. `drain_nodes()` 단일-Pod 조기 return 버그 수정 (high)
`k8s_client.py:144-167` — `forced=False`(eviction) 분기에서 첫 Pod evict 후 `return None` → 나머지 Pod 남은 채 인스턴스 종료. 또한 재귀 호출마다 `retry`가 0으로 리셋돼 `retry<2`/`retry==2` 가드가 누적 안 됨. **수정 범위**: 모든 non-daemonset Pod 순회 + 재귀를 per-pod retry 루프로 교체. 그 이상 확장 금지.

> **중요(범위 정정)**: 이 drain은 **CA/self-managed 경로 전용**(`starter.py:121`)이다. drift 모델에선 **Karpenter가 직접** cordon/drain/terminate(Eviction API, 용량 우선, PDB 준수)하므로 도구는 Karpenter 노드를 drain하지 않는다. → **`drain_nodes`를 A1 drift 구현에 절대 연결하지 말 것**(연결하면 drift 무력화). A1은 drain-free 유지.

### B2. self-managed ASG를 Launch Template로 현대화 (low)
`boto_aws.py` `add_autoscaling`이 deprecated `create_launch_configuration` 사용 → `create_launch_template_version` + `update_auto_scaling_group(LaunchTemplate=...)`. managed nodegroup의 기존 패턴 재사용.

### B3. 문서/메타데이터 정합 (low)
CLAUDE.md "1.20-1.30" → 실제 "1.33-1.36"로. vestigial해 보이는 S3Files addon JSON 정리(addon은 이미 라이브 해석).

---

## 작업 순서 (제안)

> 런타임 흐름상 Karpenter drift는 컨트롤플레인 업그레이드(A3) 시점에 시작되어 노드 단계와 동시 진행되고, 마지막에 수렴 게이트(A4)로 닫힌다. 아래는 *구현* 순서이며 실행 순서와 다르다.

1. **B1**(drain 버그) — 다른 노드 경로의 기반.
2. **A2**(CA 복원 + 공존 골격).
3. **A1**(Karpenter drift 재작성: 감지/분류/핀 갱신 + 컨트롤플레인 직후 훅) — 가장 큰 작업.
4. **A4**(drift 수렴 게이트 + 테스트).
5. **A3**(버전 현대화 + multi-hop; drift 시작 지점).
6. **B2, B3**(선택).

각 단계는 구현 전 무엇을 바꿀지 설명하고, 중요한 변경은 실행 전 재확인. TDD 원칙 적용.

## 남은 사소한 결정 (구현 중 확인)
- multi-hop 자동화를 이번에 넣을지, 안내만 할지.
- disruption budget/do-not-disrupt가 막을 때 기본 동작(대기 타임아웃 vs `--force`).

## 진행 현황 (2026-06-07)

완료(TDD, 91 tests pass):
- **B1** drain 단일-Pod 버그 + owner-aware drain(DaemonSet/mirror skip, standalone 사전 abort).
- **A2** CA deployment-scaler 복원 + 레이블 셀렉터 감지(Helm 명명 대응) + 두 경로 공존. ASG-태그 방식은 dead code라 제거.
- **A1** Karpenter drift 재작성: `karpenter.py`(classify_ami_selector / get_ec2nodeclasses / nodepools_for_nodeclasses / wait_for_karpenter_drift / handle_karpenter_drift). 컨트롤러 안 멈춤, 노드 kubelet 버전으로 **positive 확인**(거짓 성공 버그 수정), timeout+report. **mixed(alias+pinned) 토폴로지**에서 pinned 노드가 영원히 timeout을 유발하던 false-timeout도 수정 — wait는 alias EC2NodeClass를 참조하는 NodePool 노드만 검사. 반환값 `settled`/`timeout`/`no_drift` 3-state. 잘못된 terminate `upgrade_karpenter_nodes` + 그 테스트 삭제. pinned(id/name/tags)는 **경고만**(사용자 결정).

알려진 한계:
- Karpenter 테스트는 모두 mock 기반. CRD 좌표(`karpenter.k8s.aws/v1/ec2nodeclasses`, `karpenter.sh/v1/nodeclaims`)와 `classify_ami_selector`는 문서 기반이며 **실제 클러스터의 EC2NodeClass로 프로덕션 전 검증 필요**.
- `is_karpenter_present`는 deployment 이름 `karpenter` + namespace(karpenter/kube-system) 하드코딩(저위험, Helm 관례상 `karpenter`). 필요 시 CA처럼 레이블 셀렉터로 일반화 — 현재 **deferred**.
- `forced=True`는 StatefulSet/standalone Pod를 PDB 무시하고 hard-delete(--force 본질, 기존 동작).

남은 작업: A3(버전 현대화+multi-hop), A4(StatefulSet 대기 등 — Task #3), B2(LaunchConfig→LaunchTemplate), B3(S3Files/utils 로더 정리).

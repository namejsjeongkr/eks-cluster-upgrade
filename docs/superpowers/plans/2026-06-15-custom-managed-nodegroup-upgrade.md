# CUSTOM Managed NodeGroup 업그레이드 구현 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** managed 노드 그룹의 `amiType=CUSTOM` 업그레이드 경로를 구현한다. AWS는 CUSTOM NG에 version-only 업데이트를 거부하므로(`Launch template details can't be null for Custom ami type node group`), 새 LT 버전(목표 k8s AMI로 ImageId만 교체, KMS BDM/UserData는 SourceVersion="$Latest"로 자동 보존)을 만들고 그 LT로 `UpdateNodegroupVersion`을 호출한다.

**Architecture:** `ManagedNodeGroup.update` (eksupgrade/models/eks.py)의 CUSTOM 분기를 채운다. self-managed 경로에 이미 검증된 `update_current_launch_template_ami`(SourceVersion 병합)와 `get_latest_ami`를 재사용한다. CUSTOM이면: 현재 LT에서 os_type 추출 → 목표 AMI resolve → 새 LT 버전 생성(KMS 자동 상속) → `launchTemplate={id, version:"$Latest"}`로 NG 업데이트.

**Tech Stack:** Python, boto3 (eks/ec2), 기존 `eksupgrade.src.latest_ami.get_latest_ami` / `eksupgrade.src.self_managed.update_current_launch_template_ami`, pytest + moto/unittest.mock.

참고:
- 실제 클러스터 LT: `lt-073246c8960528117` v1, ImageId `ami-074e925cf02f4ca37`(1.32), BDM 2개(`/dev/xvda`,`/dev/xvdb`) KMS `8b85b859-...` 암호화, UserData 있음.
- 목표 1.33 Bottlerocket AMI: `ami-0d105aacda200c54d` (preflight resolve 확인).
- Terraform source of truth: `terraform.tfvars`의 `bottlerocket_launch_template_configurations` (image_id + block_device_mappings + kms_key_id). 업그레이드 후 tfvars의 `cluster_version`/`image_id`를 1.33으로 sync 예정(별도, 코드 외).

## 확인된 기존 인터페이스

- `ManagedNodeGroup`: `.name`, `.version`, `.ami_type`, `.launch_template` (`{"id","version","name"}` dict), `.cluster.target_version`, `.cluster.region`, `.cluster.name`, `.eks_client`.
- `ManagedNodeGroup.update(version="", release_version="", force=False, client_request_id="", launch_template=None, wait=True)` — 현재 CUSTOM 분기(eks.py ~369-370)는 `echo_error`만 하고 아무 동작 안 함(버그).
- `eksupgrade.src.latest_ami.get_latest_ami(cluster_version, instance_type, image_to_search, region) -> str` — bottlerocket 분기는 instance_type에 "bottlerocket"이 들어가야 동작(`""`면 실패). preflight에서 검증된 호출: `get_latest_ami(target, "bottlerocket", "bottlerocket", region)`.
- `eksupgrade.src.self_managed.update_current_launch_template_ami(lt_id, latest_ami, region)` — `create_launch_template_version(LaunchTemplateId=lt_id, SourceVersion="$Latest", VersionDescription="Latest-AMI", LaunchTemplateData={"ImageId": latest_ami})`. SourceVersion 병합으로 KMS BDM/UserData 보존. **코드 리뷰는 됐으나 라이브 실행은 안 됨 — "검증됨"으로 간주하지 말 것.** 현재 반환값 None. 호출처는 self_managed.py:86 단 1곳(반환값 미사용)이라 반환값 추가는 하위 호환.

### 핵심 제약 (advisor + 실측 확인)

- **두 managed NG가 동일 LT를 공유한다**: `bsdapne2-omdw-ng-bottlerocket_ondemand_t3medium`와 `..._spot_t3medium` 모두 `lt-073246c8960528117` v1. 따라서 `launchTemplate={version:"$Latest"}`는 **race 위험** — NG1이 v2를 만들고, NG2가 v3를 만든 뒤, EKS가 NG1의 "$Latest"를 처리할 때 v3에 바인딩될 수 있다. 또한 EKS UpdateNodegroupVersion이 managed NG LT에 "$Latest"를 수락하는지 불확실(ASG는 되지만 managed NG는 숫자를 요구할 수 있음).
  → **`create_launch_template_version`이 만든 구체 VersionNumber를 캡처해 그 정수를 launchTemplate.version으로 사용한다.** "$Latest" 금지.
- **계정/리전/KMS 독립**: 코드는 KMS Key를 명시하지 않는다. AMI/KMS BDM 모두 "현재 NG가 쓰는 기존 LT"에서 가져온다(AMI는 OS타입 추출 후 목표버전 resolve, KMS BDM은 SourceVersion 병합으로 자동 상속). 그래서 계정마다 다른 KMS 키여도 동작한다.
- **ImageLocation 확인됨**: `ami-074e925cf02f4ca37` → `amazon/bottlerocket-aws-k8s-1.32-x86_64-v1.32.0-cacc4ce9` → `bottlerocket`+`x86_64` 포함 → os_type 추출 경로가 올바른 분기를 타고 1.33 AMI(`ami-0d105aacda200c54d`)로 resolve.
- self_managed.py의 os_type 추출 패턴(참고): 현재 LT의 ImageId → `ec2.describe_images(ImageIds=[ami])["Images"][0]["ImageLocation"]` → 그게 os_type.

## 파일 구조

- Modify: `eksupgrade/models/eks.py` — `ManagedNodeGroup`에 CUSTOM AMI resolve + LT 버전 생성 헬퍼 추가, `update`의 CUSTOM 경로 구현. `get_latest_ami` / `update_current_launch_template_ami` import 추가.
- Test: `tests/test_models_eks.py` — CUSTOM 경로 단위 테스트(mock).

---

### Task 1: ManagedNodeGroup CUSTOM 업그레이드 경로 구현

**Files:**
- Modify: `eksupgrade/models/eks.py`
- Test: `tests/test_models_eks.py`

- [ ] **Step 1: Write the failing test**

READ `tests/test_models_eks.py` first for the ManagedNodeGroup construction/mocking pattern. Add tests:

```python
def test_managed_ng_custom_resolves_ami_and_creates_lt_version(...):
    """CUSTOM NG: resolve target AMI, create new LT version (SourceVersion merge), update with launchTemplate."""
    # Build a ManagedNodeGroup with ami_type="CUSTOM", launch_template={"id":"lt-1","version":"1"},
    # version="1.32", cluster.target_version="1.33", cluster.region="ap-northeast-2".
    # Patch:
    #   eksupgrade.models.eks.get_latest_ami -> "ami-new"
    #   eksupgrade.models.eks.update_current_launch_template_ami -> 2  (returns the new LT version number)
    #   the ec2 describe_images call used for os_type extraction -> ImageLocation
    #     "amazon/bottlerocket-aws-k8s-1.32-x86_64-v1.32.0" (realistic)
    #   ng.eks_client.update_nodegroup_version -> {"update": {"id":"u1","status":"InProgress"}}
    # Call ng.update(wait=False)
    # Assert:
    #   update_current_launch_template_ami was called once with ("lt-1", "ami-new", "ap-northeast-2")
    #   update_nodegroup_version was called with launchTemplate == {"id":"lt-1","version":"2"}
    #     (the CONCRETE returned version as a string, NOT "$Latest")
    #   update_kwargs had NO top-level "version" and NO "releaseVersion" (AWS rejects version + CUSTOM LT)
    #   get_latest_ami was called with instance_type containing "bottlerocket" (not "")

def test_managed_ng_non_custom_unchanged(...):
    """Non-CUSTOM NG still does a version-only update (regression guard)."""
    # ami_type="AL2023_x86_64", no launch_template; ng.update(wait=False)
    # Assert update_nodegroup_version called with version=target_version, no launchTemplate.
```

Adapt construction to the file's existing fixtures. The key assertions: CUSTOM → resolves AMI, creates LT version via the helper, updates with `launchTemplate={id, version:"$Latest"}` and no `version`; non-CUSTOM → unchanged version-only path.

- [ ] **Step 2: Run to verify failure**

`poetry run pytest tests/test_models_eks.py -k managed_ng_custom -v`
Expected: FAIL — current CUSTOM branch only echo_errors; update_current_launch_template_ami not called / update_nodegroup_version not called with launchTemplate.

- [ ] **Step 3: Implement**

FIRST, modify `eksupgrade/src/self_managed.py` so `update_current_launch_template_ami` RETURNS the concrete new version number (currently returns None). `create_launch_template_version` response includes `LaunchTemplateVersion.VersionNumber`:
```python
def update_current_launch_template_ami(lt_id: str, latest_ami: str, region: str) -> int:
    """Create a new launch template version with the new AMI; return its version number.

    SourceVersion="$Latest" merges the latest existing version, preserving its
    block device mappings (incl. KMS-encrypted volumes), UserData, etc., and only
    overrides ImageId.
    """
    ec2 = boto3.client("ec2", region_name=region)
    response = ec2.create_launch_template_version(
        LaunchTemplateId=lt_id,
        SourceVersion="$Latest",
        VersionDescription="Latest-AMI",
        LaunchTemplateData={"ImageId": latest_ami},
    )
    new_version = response["LaunchTemplateVersion"]["VersionNumber"]
    echo_info(f"New launch template version {new_version} created with AMI {latest_ami}")
    return new_version
```
The one existing caller (self_managed.py:86) ignores the return value, so this is backward compatible.

THEN in `eksupgrade/models/eks.py`, add imports near other `from .src` / `from eksupgrade.src` imports:
```python
from eksupgrade.src.latest_ami import get_latest_ami
from eksupgrade.src.self_managed import update_current_launch_template_ami
```
(Match the file's existing import style. Circular import is safe: self_managed imports only latest_ami + utils, never models/eks — confirmed. If one appears anyway, import inside the method.)

Add a helper on `ManagedNodeGroup` to resolve the target AMI from the current LT's OS type:
```python
    def _resolve_custom_target_ami(self) -> str:
        """Resolve the target-version AMI for this CUSTOM node group from its launch template's OS type."""
        ec2 = boto3.client("ec2", region_name=self.cluster.region)
        lt_id = self.launch_template["id"]
        lt_version = str(self.launch_template["version"])
        lt_data = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id, Versions=[lt_version])
        current_ami = lt_data["LaunchTemplateVersions"][0]["LaunchTemplateData"]["ImageId"]
        os_type = ec2.describe_images(ImageIds=[current_ami])["Images"][0]["ImageLocation"]
        if isinstance(os_type, str) and "Windows_Server" in os_type:
            os_type = os_type[:46]
        return get_latest_ami(
            cluster_version=self.cluster.target_version,
            instance_type=os_type,
            image_to_search=os_type,
            region=self.cluster.region,
        )
```

Then replace the broken CUSTOM branch in `update`. The current version logic (eks.py ~360-368) is:
```python
        if not launch_template:
            update_kwargs["version"] = version or self.cluster.target_version
        elif launch_template and not version:
            update_kwargs["launchTemplate"] = launch_template
        elif launch_template and (self.ami_type != "CUSTOM" and version):
            update_kwargs["launchTemplate"] = launch_template
            update_kwargs["version"] = version
        elif launch_template and (self.ami_type == "CUSTOM" and version):
            echo_error("Version and launch template provided to managed nodegroug update with custom AMI!")
```
Add a CUSTOM auto-handle BEFORE the `if not launch_template:` chain so a plain `update(wait=...)` call (no launch_template, no version) on a CUSTOM NG resolves+creates+points at the LT. Restructure to:
```python
        if self.ami_type == "CUSTOM" and not launch_template:
            # AWS rejects version-only updates for CUSTOM amiType. Resolve the
            # target-version AMI, create a new LT version (SourceVersion="$Latest"
            # so KMS-encrypted block device mappings and UserData are preserved),
            # then point the node group at that EXACT new version number. We must
            # NOT use "$Latest" here: both managed NGs share one launch template,
            # so "$Latest" could bind a node group to a version created for the
            # other group (race) — and EKS may not accept "$Latest" for managed-NG
            # launch templates at all.
            target_ami = self._resolve_custom_target_ami()
            lt_id = self.launch_template["id"]
            new_lt_version = update_current_launch_template_ami(lt_id, target_ami, self.cluster.region)
            update_kwargs["launchTemplate"] = {"id": lt_id, "version": str(new_lt_version)}
        elif not launch_template:
            update_kwargs["version"] = version or self.cluster.target_version
        elif launch_template and not version:
            update_kwargs["launchTemplate"] = launch_template
        elif launch_template and (self.ami_type != "CUSTOM" and version):
            update_kwargs["launchTemplate"] = launch_template
            update_kwargs["version"] = version
        elif launch_template and (self.ami_type == "CUSTOM" and version):
            echo_error("Version and launch template provided to managed nodegroup update with custom AMI!")
```
IMPORTANT: for the CUSTOM path, `update_kwargs` must NOT contain `version` or `releaseVersion` (AWS rejects version + CUSTOM LT). Confirm `release_version` is empty in the upgrade_nodegroups call path (it calls `update(wait=wait)` with no release_version, so the `if release_version:` block at ~372 won't add it — good). Leave the rest of `update` (the update_nodegroup_version call, error handling, wait) unchanged.

- [ ] **Step 4: Run to verify pass**

`poetry run pytest tests/test_models_eks.py -k managed_ng -v` (custom + non-custom pass)
Then full module `poetry run pytest tests/test_models_eks.py -v`.
Then full suite `poetry run pytest -q` (no regressions; report count).

- [ ] **Step 5: Lint + commit**

```
poetry run black eksupgrade/models/eks.py tests/test_models_eks.py
poetry run isort eksupgrade/models/eks.py tests/test_models_eks.py
poetry run ruff check eksupgrade/models/eks.py tests/test_models_eks.py
git add eksupgrade/models/eks.py tests/test_models_eks.py
git commit -m "feat: upgrade CUSTOM managed node groups via new launch template version"
```

- [ ] **Step 6: self_managed regression guard**

The return-type change to `update_current_launch_template_ami` must not break the self-managed path. If `tests/` has a test that patches/asserts that function, update it to tolerate/return an int. Run `poetry run pytest tests/ -k self_managed -v` (or the relevant self-managed test file) and confirm green. If no such test exists, note that and rely on the existing suite.

---

## Live verification gate (manual, between create and node roll)

mock 테스트는 페이로드 모양만 증명한다. KMS 보존과 AMI 정확성은 **노드 롤 전에** 다음으로 확인한다 (advisor 권장, read-only + reversible):

1. 코드 배포 후, 실제 업그레이드 재실행 시 `create_launch_template_version`이 새 LT 버전을 만든다(노드는 아직 안 바뀜 — `UpdateNodegroupVersion`이 처리되기 전까지).
2. 만들어진 새 LT 버전을 describe로 검사:
   ```sh
   aws ec2 describe-launch-template-versions --launch-template-id lt-073246c8960528117 \
     --versions '$Latest' --region ap-northeast-2 --profile bsd-prod \
     --query 'LaunchTemplateVersions[0].LaunchTemplateData.{ImageId:ImageId,BDM:BlockDeviceMappings}'
   ```
   확인: `ImageId == ami-0d105aacda200c54d`(1.33), 그리고 BDM 2개(`/dev/xvda`,`/dev/xvdb`)가 여전히 `Encrypted:true` + `KmsKeyId: ...8b85b859...`.
3. 위가 맞으면 노드 롤(AWS managed rolling update)이 안전하게 진행. 틀리면 새 LT 버전을 삭제하고 롤백(노드 무변경 상태이므로 안전).

**efs-csi 주의**: 노드 교체는 AWS managed rolling update이며 PDB를 존중한다. `efs-csi-controller`에 PDB가 없으면 업그레이드가 **막히지는 않지만** 두 replica가 동시에 교체될 수 있다(가용성 일시 저하). 필요 시 노드 롤 전 PDB(`minAvailable:1`) 추가.

**TF sync 주의**: 이후 `terraform apply` 시, TF는 자신이 소유한 `aws_launch_template`에 새 버전을 만들고 NG의 LT 포인터를 재설정한다. 사용자가 drift를 수용했으므로 정상 흐름 — sync 시 TF가 LT 버전을 새로 만들고 NG를 re-pin한다는 점만 인지.

---

## Self-Review

**Spec coverage:** CUSTOM NG version-only 거부 해결 → CUSTOM 분기에서 AMI resolve + LT 버전 생성 + launchTemplate 업데이트 ✅; KMS BDM 보존 → `update_current_launch_template_ami`의 SourceVersion="$Latest" 병합(기존 BDM 자동 상속) ✅; 원복 가능 → 새 LT 버전 추가(v1 보존) ✅; non-CUSTOM 회귀 방지 → 기존 경로 유지 + 테스트 ✅.

**Placeholder scan:** 모든 스텝 실제 코드 포함.

**Type consistency:** `_resolve_custom_target_ami() -> str`, `update_current_launch_template_ami(lt_id, latest_ami, region)`, `get_latest_ami(cluster_version, instance_type, image_to_search, region)`, `launchTemplate={"id","version"}` — 일관.

**검증 한계 (advisor):** mock 테스트는 "올바른 API 페이로드를 만드는지"까지만 증명. AWS가 CUSTOM NG에 이 launchTemplate 페이로드를 실제로 수락하는지 + 새 노드가 KMS 암호화로 정상 기동하는지는 **prod 노드 롤이 유일한 검증**. dry-run 없음.

**알려진 의존:** self_managed.py를 models/eks.py에서 import — 순환 import 없음 확인 필요(self_managed는 latest_ami/utils만 import). 문제 시 메서드 내부 import로 회피.

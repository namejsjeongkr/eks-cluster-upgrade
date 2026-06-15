"""Test the EKS model logic."""

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api_client import ApiClient

from eksupgrade.exceptions import EksException
from eksupgrade.models.eks import Cluster, ClusterAddon, ManagedNodeGroup


def test_cluster_resource(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the cluster resource."""
    cluster_resource = Cluster.get(cluster_name, region)
    cluster_dict = cluster_resource.to_dict()
    assert cluster_dict
    assert isinstance(cluster_dict, dict)
    assert cluster_dict["version"] == "1.23"
    assert len(cluster_dict.keys()) == 20
    assert cluster_resource.name == cluster_resource.cluster_name


def test_cluster_resource_eks_client(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the cluster resource."""
    cluster_resource = Cluster.get(cluster_name, region)

    assert cluster_resource.eks_client
    assert cluster_resource.eks_client.meta.region_name == "us-east-1"


def test_cluster_resource_core_client(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the cluster resource."""
    cluster_resource = Cluster.get(cluster_name, region)
    assert isinstance(cluster_resource.core_api_client, CoreV1Api)
    assert isinstance(cluster_resource.core_api_client.api_client, ApiClient)


def test_cluster_addon_resource(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the cluster addon resource."""
    cluster_resource = Cluster.get(cluster_name, region)
    addon_resource = ClusterAddon(
        arn="abc", name="coredns", cluster=cluster_resource, region=region, owner="amazon", publisher="amazon"
    )
    addon_dict = addon_resource.to_dict()
    assert isinstance(addon_dict, dict)
    assert addon_dict["arn"] == "abc"
    assert addon_resource.name == "coredns"
    assert not addon_dict["resource_id"]
    assert not addon_dict["tags"]
    assert len(addon_dict.keys()) == 20
    assert addon_resource.name == addon_resource.addon_name
    assert not addon_resource._addon_update_kwargs
    assert isinstance(addon_resource._addon_update_kwargs, dict)


def test_cluster_addon_resource_update_kwargs(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the cluster addon resource."""
    cluster_resource = Cluster.get(cluster_name, region)
    addon_resource = ClusterAddon(
        arn="abc", name="coredns", cluster=cluster_resource, region=region, owner="amazon", publisher="amazon"
    )
    addon_resource.service_account_role_arn = "123"
    addon_resource.configuration_values = "123"
    assert addon_resource._addon_update_kwargs
    assert isinstance(addon_resource._addon_update_kwargs, dict)
    assert "serviceAccountRoleArn" in addon_resource._addon_update_kwargs.keys()
    assert "configurationValues" in addon_resource._addon_update_kwargs.keys()


# def test_cluster_requires_cluster_decorator(eks_client, eks_cluster, cluster_name, region) -> None:
#     """Test the cluster addon resource."""

#     @requires_cluster
#     def decorator_test(addon):
#         return addon

#     # Validate without populated cluster.
#     cluster_resource = Cluster(arn="123", version="1.24", target_version="1.25")
#     addon_resource = ClusterAddon(
#         arn="abc", name="coredns", cluster=cluster_resource, region=region, owner="amazon", publisher="amazon"
#     )
#     assert not addon_resource.cluster.name
#     assert decorator_test(addon_resource) is None

#     # Validate with populated cluster.
#     addon_resource.cluster = Cluster.get(cluster_name, region)
#     assert addon_resource.cluster.name
#     assert decorator_test(addon_resource)


# def test_cluster_addon_resource_no_cluster(eks_client, eks_cluster, cluster_name, region) -> None:
#     """Test the cluster addon resource."""
#     cluster_resource = Cluster(arn="123", version="1.24", target_version="1.25")
#     addon_resource = ClusterAddon(
#         arn="abc", name="coredns", cluster=cluster_resource, region=region, owner="amazon", publisher="amazon"
#     )
#     addon_dict = addon_resource.to_dict()
#     assert isinstance(addon_dict, dict)
#     assert addon_dict["arn"] == "abc"
#     assert addon_resource.name == "coredns"
#     assert not addon_dict["resource_id"]
#     assert not addon_dict["tags"]
#     assert len(addon_dict.keys()) == 17


def test_addon_version_parsing_tolerates_eksbuild_suffix(eks_client, eks_cluster, cluster_name, region) -> None:
    """Real EKS addon versions carry a -eksbuild.N suffix that packaging rejects unless stripped."""
    cluster_resource = Cluster.get(cluster_name, region)
    cluster_resource.latest_addons = False
    addon_resource = ClusterAddon(
        arn="abc",
        name="coredns",
        cluster=cluster_resource,
        region=region,
        owner="amazon",
        publisher="amazon",
        version="v1.39.0-eksbuild.1",
    )
    # Override the cached AWS-backed properties with realistic eksbuild-suffixed strings.
    addon_resource.__dict__["available_versions"] = ["v1.61.1-eksbuild.1", "v1.39.0-eksbuild.1"]
    addon_resource.__dict__["default_version"] = "v1.61.1-eksbuild.1"

    # These must NOT raise packaging.version.InvalidVersion:
    assert addon_resource.sorted_versions[0] == "v1.61.1-eksbuild.1"
    assert addon_resource.needs_upgrade is True
    assert addon_resource.target_version == "v1.61.1-eksbuild.1"


def test_vpc_cni_graduated_target_tolerates_eksbuild_suffix(eks_client, eks_cluster, cluster_name, region) -> None:
    """vpc-cni steps one minor at a time; the graduated branch parses self.next_minor,
    which is a raw vX.Y.Z-eksbuild.N string — must be stripped before Version().

    With current minor=10 and target minor=12 (two apart), within_target_minor is False,
    so the vpc-cni graduated branch is taken and _addon_semver(self.next_minor) is called.
    A passing assertion proves the branch ran (it would raise InvalidVersion before the fix,
    or return the +2 target "v1.12.6-eksbuild.2" if the branch were skipped).
    """
    cluster_resource = Cluster.get(cluster_name, region)
    cluster_resource.latest_addons = False
    addon_resource = ClusterAddon(
        arn="abc",
        name="vpc-cni",
        cluster=cluster_resource,
        region=region,
        owner="amazon",
        publisher="amazon",
        version="v1.10.4-eksbuild.1",
    )
    # Override cached AWS-backed properties with realistic eksbuild-suffixed strings.
    # Minor spread: current=10, next=11, target=12 → within_target_minor is False → graduated branch taken.
    addon_resource.__dict__["available_versions"] = [
        "v1.12.6-eksbuild.2",
        "v1.11.4-eksbuild.1",
        "v1.10.4-eksbuild.1",
    ]
    addon_resource.__dict__["default_version"] = "v1.12.6-eksbuild.2"

    # Must NOT raise InvalidVersion, and must step to next minor (11), not jump +2 to target (12).
    assert addon_resource.target_version == "v1.11.4-eksbuild.1"


def _custom_managed_ng(eks_client, region):
    """Build a CUSTOM amiType managed node group wired to a mocked eks client."""
    cluster = Cluster(arn="abc", name="eks-test", version="1.32", target_version="1.33", region="ap-northeast-2")
    cluster.eks_client = MagicMock()
    ng = ManagedNodeGroup(
        arn="abc",
        cluster=cluster,
        name="custom-ng",
        version="1.32",
        status="ACTIVE",
        ami_type="CUSTOM",
        launch_template={"id": "lt-1", "version": "1", "name": "lt-custom"},
        region="ap-northeast-2",
    )
    ng.eks_client = MagicMock()
    ng.eks_client.update_nodegroup_version.return_value = {"update": {"id": "u1", "status": "InProgress"}}
    return ng


def test_managed_ng_custom_resolves_ami_and_uses_concrete_lt_version(ec2_client, eks_client, region) -> None:
    """CUSTOM managed NG: resolve AMI, create new LT version, point NG at the concrete version."""
    ng = _custom_managed_ng(eks_client, region)

    with (
        patch("eksupgrade.models.eks.update_current_launch_template_ami", return_value=2) as mock_update_lt,
        patch.object(ManagedNodeGroup, "_resolve_custom_target_ami", return_value="ami-new"),
    ):
        ng.update(wait=False)

    mock_update_lt.assert_called_once_with("lt-1", "ami-new", "ap-northeast-2")

    call = ng.eks_client.update_nodegroup_version.call_args
    assert call.kwargs["launchTemplate"] == {"id": "lt-1", "version": "2"}
    assert "version" not in call.kwargs
    assert "releaseVersion" not in call.kwargs


def test_managed_ng_non_custom_still_version_only(ec2_client, eks_client, region) -> None:
    """Non-CUSTOM managed NG: version-only update, no launchTemplate."""
    cluster = Cluster(arn="abc", name="eks-test", version="1.32", target_version="1.33", region="ap-northeast-2")
    cluster.eks_client = MagicMock()
    ng = ManagedNodeGroup(
        arn="abc",
        cluster=cluster,
        name="al2023-ng",
        version="1.32",
        status="ACTIVE",
        ami_type="AL2023_x86_64",
        region="ap-northeast-2",
    )
    ng.eks_client = MagicMock()
    ng.eks_client.update_nodegroup_version.return_value = {"update": {"id": "u1", "status": "InProgress"}}

    ng.update(wait=False)

    call = ng.eks_client.update_nodegroup_version.call_args
    assert call.kwargs["version"] == "1.33"
    assert "launchTemplate" not in call.kwargs


def test_resolve_custom_target_ami_passes_os_hint(ec2_client, eks_client, region) -> None:
    """_resolve_custom_target_ami must pass the OS-derived instance_type hint to get_latest_ami."""
    ng = _custom_managed_ng(eks_client, region)

    fake_ec2 = MagicMock()
    fake_ec2.describe_launch_template_versions.return_value = {
        "LaunchTemplateVersions": [{"LaunchTemplateData": {"ImageId": "ami-current"}}]
    }
    fake_ec2.describe_images.return_value = {
        "Images": [{"ImageLocation": "amazon/bottlerocket-aws-k8s-1.32-x86_64-v1.32.0"}]
    }

    with (
        patch("eksupgrade.models.eks.boto3.client", return_value=fake_ec2),
        patch("eksupgrade.models.eks.get_latest_ami", return_value="ami-new") as mock_get_ami,
    ):
        result = ng._resolve_custom_target_ami()

    assert result == "ami-new"
    call = mock_get_ami.call_args
    assert "bottlerocket" in call.kwargs["instance_type"]
    assert call.kwargs["cluster_version"] == "1.33"


def test_resolve_custom_target_ami_raises_when_image_deregistered(ec2_client, eks_client, region) -> None:
    """A deregistered/missing current AMI must raise a clear error, not an opaque IndexError."""
    ng = _custom_managed_ng(eks_client, region)

    fake_ec2 = MagicMock()
    fake_ec2.describe_launch_template_versions.return_value = {
        "LaunchTemplateVersions": [{"LaunchTemplateData": {"ImageId": "ami-current"}}]
    }
    fake_ec2.describe_images.return_value = {"Images": []}

    with patch("eksupgrade.models.eks.boto3.client", return_value=fake_ec2):
        with pytest.raises(EksException) as exc_info:
            ng._resolve_custom_target_ami()

    message = str(exc_info.value)
    assert "ami-current" in message
    assert "deregister" in message.lower()


def test_resolve_custom_target_ami_raises_on_unresolvable_os(ec2_client, eks_client, region) -> None:
    """An unresolvable OS (get_latest_ami -> "NAN") must raise, not silently return the sentinel."""
    ng = _custom_managed_ng(eks_client, region)

    fake_ec2 = MagicMock()
    fake_ec2.describe_launch_template_versions.return_value = {
        "LaunchTemplateVersions": [{"LaunchTemplateData": {"ImageId": "ami-current"}}]
    }
    fake_ec2.describe_images.return_value = {"Images": [{"ImageLocation": "amazon/some-unknown-os-image"}]}

    with (
        patch("eksupgrade.models.eks.boto3.client", return_value=fake_ec2),
        patch("eksupgrade.models.eks.get_latest_ami", return_value="NAN"),
    ):
        with pytest.raises(EksException) as exc_info:
            ng._resolve_custom_target_ami()

    assert "target AMI" in str(exc_info.value)

"""Test the EKS model logic."""

from kubernetes.client.api.core_v1_api import CoreV1Api
from kubernetes.client.api_client import ApiClient

from eksupgrade.models.eks import Cluster, ClusterAddon


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

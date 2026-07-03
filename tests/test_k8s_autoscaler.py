"""Test Cluster-Autoscaler deployment controls in k8s_client.

is_cluster_auto_scaler_present() detects the cluster-autoscaler Deployment and
returns its replica count; cluster_auto_enable_disable() scales it to 0 (pause)
or back to the saved count (start) so it doesn't fight a node roll.
"""

from unittest.mock import MagicMock, patch

import pytest

from eksupgrade.src.k8s_client import cluster_auto_enable_disable, is_cluster_auto_scaler_present

CLUSTER = "test-cluster"
REGION = "us-east-1"

_LOADING = "eksupgrade.src.k8s_client.loading_config"
_K8S = "eksupgrade.src.k8s_client.client"


def _deployment(name: str, replicas: int, labels: dict | None = None, namespace: str = "kube-system") -> MagicMock:
    dep = MagicMock()
    dep.metadata.name = name
    dep.metadata.namespace = namespace
    dep.metadata.labels = labels or {}
    dep.spec.replicas = replicas
    return dep


def _deployment_list(*deps: MagicMock) -> MagicMock:
    response = MagicMock()
    response.items = list(deps)
    return response


class TestIsClusterAutoScalerPresent:
    @patch(_K8S)
    @patch(_LOADING)
    def test_returns_true_and_replicas_when_present(self, mock_loading, mock_k8s):
        apps = mock_k8s.AppsV1Api.return_value
        apps.list_deployment_for_all_namespaces.return_value = _deployment_list(
            _deployment("some-other", 1), _deployment("cluster-autoscaler", 2)
        )

        result = is_cluster_auto_scaler_present(CLUSTER, REGION)

        assert result[0] is True
        assert result[1] == 2

    @patch(_K8S)
    @patch(_LOADING)
    def test_returns_false_when_absent(self, mock_loading, mock_k8s):
        apps = mock_k8s.AppsV1Api.return_value
        apps.list_deployment_for_all_namespaces.return_value = _deployment_list(_deployment("coredns", 2))

        result = is_cluster_auto_scaler_present(CLUSTER, REGION)

        assert result[0] is False
        assert result[1] == 0

    @patch(_K8S)
    @patch(_LOADING)
    def test_detects_helm_named_ca_via_label(self, mock_loading, mock_k8s):
        """Helm names CA <release>-aws-cluster-autoscaler; detect it via label."""
        apps = mock_k8s.AppsV1Api.return_value
        apps.list_deployment_for_all_namespaces.return_value = _deployment_list(
            _deployment(
                "my-release-aws-cluster-autoscaler",
                4,
                labels={"app.kubernetes.io/name": "aws-cluster-autoscaler"},
                namespace="autoscaler-ns",
            )
        )

        result = is_cluster_auto_scaler_present(CLUSTER, REGION)

        assert result[0] is True
        assert result[1] == 4
        # the discovered name and namespace must be returned for accurate pause/resume
        assert result[2] == "my-release-aws-cluster-autoscaler"
        assert result[3] == "autoscaler-ns"


class TestClusterAutoEnableDisable:
    @patch(_K8S)
    @patch(_LOADING)
    def test_pause_scales_to_zero(self, mock_loading, mock_k8s):
        apps = mock_k8s.AppsV1Api.return_value

        cluster_auto_enable_disable(CLUSTER, "pause", mx_val=3, region=REGION)

        _, kwargs = apps.patch_namespaced_deployment.call_args
        assert kwargs["name"] == "cluster-autoscaler"
        assert kwargs["body"]["spec"]["replicas"] == 0

    @patch(_K8S)
    @patch(_LOADING)
    def test_start_restores_replicas(self, mock_loading, mock_k8s):
        apps = mock_k8s.AppsV1Api.return_value

        cluster_auto_enable_disable(CLUSTER, "start", mx_val=3, region=REGION)

        _, kwargs = apps.patch_namespaced_deployment.call_args
        assert kwargs["body"]["spec"]["replicas"] == 3

    @patch(_K8S)
    @patch(_LOADING)
    def test_uses_provided_name_and_namespace(self, mock_loading, mock_k8s):
        """Helm/non-default installs must be paused by their actual name + namespace."""
        apps = mock_k8s.AppsV1Api.return_value

        cluster_auto_enable_disable(
            CLUSTER,
            "pause",
            mx_val=2,
            region=REGION,
            name="my-release-aws-cluster-autoscaler",
            namespace="autoscaler-ns",
        )

        _, kwargs = apps.patch_namespaced_deployment.call_args
        assert kwargs["name"] == "my-release-aws-cluster-autoscaler"
        assert kwargs["namespace"] == "autoscaler-ns"

    @patch(_K8S)
    @patch(_LOADING)
    def test_invalid_operation_raises(self, mock_loading, mock_k8s):
        with pytest.raises(NotImplementedError):
            cluster_auto_enable_disable(CLUSTER, "bogus", mx_val=1, region=REGION)

"""Test Karpenter node upgrade logic (upgrade_karpenter_nodes).

Tests verify the complete upgrade flow for each Karpenter-managed node:
  1. Cordon  — unschedule_old_nodes() prevents new pods from landing
  2. Drain   — drain_nodes() evicts existing pods (honours PDB unless forced)
  3. Terminate — EC2 instance is terminated; Karpenter re-provisions on resume

All kubernetes client calls are patched with unittest.mock so no real cluster
or AWS credentials are required.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from eksupgrade.src.k8s_client import upgrade_karpenter_nodes

CLUSTER = "test-cluster"
REGION = "us-east-1"

# Stable patch targets — always patch at the import location
_LOADING = "eksupgrade.src.k8s_client.loading_config"
_GET_NODES = "eksupgrade.src.k8s_client.get_karpenter_nodes"
_K8S = "eksupgrade.src.k8s_client.client"
_BOTO3 = "eksupgrade.src.k8s_client.boto3.client"
_UNSCHEDULE = "eksupgrade.src.k8s_client.unschedule_old_nodes"
_DRAIN = "eksupgrade.src.k8s_client.drain_nodes"


def _make_node(instance_id: str) -> MagicMock:
    """Return a mock kubernetes Node with a well-formed provider_id."""
    node = MagicMock()
    node.spec.provider_id = f"aws:///us-east-1a/{instance_id}"
    return node


# ---------------------------------------------------------------------------
# Helpers: decorators applied in reverse order (bottom = outermost wrapper)
# ---------------------------------------------------------------------------

def _standard_patches(fn):
    """Apply the six patches needed by most tests in a consistent order."""
    for decorator in reversed([
        patch(_LOADING),
        patch(_GET_NODES),
        patch(_BOTO3),
        patch(_K8S),
        patch(_UNSCHEDULE),
        patch(_DRAIN),
    ]):
        fn = decorator(fn)
    return fn


# ---------------------------------------------------------------------------
# No-op scenario
# ---------------------------------------------------------------------------

class TestNoKarpenterNodes:
    """When no Karpenter nodes exist the function should return without action."""

    @patch(_GET_NODES, return_value=[])
    @patch(_LOADING)
    def test_returns_early_with_no_nodes(self, mock_loading, mock_get_nodes):
        upgrade_karpenter_nodes(CLUSTER, REGION)

        mock_get_nodes.assert_called_once_with(CLUSTER, REGION)
        mock_loading.assert_not_called()


# ---------------------------------------------------------------------------
# Single-node upgrade
# ---------------------------------------------------------------------------

class TestSingleNodeUpgrade:
    """Verify cordon → drain → terminate sequence and argument correctness."""

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_cordon_before_drain(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        """Cordon must happen before drain — wrong order risks pod disruption."""
        mock_get_nodes.return_value = ["node-1"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node("i-abc")
        mock_boto3.return_value = MagicMock()

        call_order: list[str] = []
        mock_unschedule.side_effect = lambda **_: call_order.append("cordon")
        mock_drain.side_effect = lambda **_: call_order.append("drain")

        upgrade_karpenter_nodes(CLUSTER, REGION)

        assert call_order == ["cordon", "drain"], (
            f"Expected cordon → drain, got: {call_order}"
        )

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_correct_arguments_to_all_calls(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        node_name = "ip-10-0-1-1.ec2.internal"
        instance_id = "i-0abc123def456789"
        mock_get_nodes.return_value = [node_name]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node(instance_id)
        mock_ec2 = MagicMock()
        mock_boto3.return_value = mock_ec2

        upgrade_karpenter_nodes(CLUSTER, REGION, forced=False)

        mock_unschedule.assert_called_once_with(
            cluster_name=CLUSTER, node_name=node_name, region=REGION
        )
        mock_drain.assert_called_once_with(
            cluster_name=CLUSTER, node_name=node_name, forced=False, region=REGION
        )
        mock_ec2.terminate_instances.assert_called_once_with(InstanceIds=[instance_id])

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_forced_flag_propagated_to_drain(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        """--force must reach drain_nodes so it can bypass PDB restrictions."""
        mock_get_nodes.return_value = ["node-force"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node("i-force")
        mock_boto3.return_value = MagicMock()

        upgrade_karpenter_nodes(CLUSTER, REGION, forced=True)

        mock_drain.assert_called_once_with(
            cluster_name=CLUSTER, node_name="node-force", forced=True, region=REGION
        )

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_instance_id_parsed_from_provider_id(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        """provider_id format: aws:///az/i-xxxxx — instance ID is the last segment."""
        instance_id = "i-0deadbeef12345678"
        mock_get_nodes.return_value = ["node-x"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node(instance_id)
        mock_ec2 = MagicMock()
        mock_boto3.return_value = mock_ec2

        upgrade_karpenter_nodes(CLUSTER, REGION)

        mock_ec2.terminate_instances.assert_called_once_with(InstanceIds=[instance_id])


# ---------------------------------------------------------------------------
# Multiple nodes
# ---------------------------------------------------------------------------

class TestMultipleNodeUpgrade:
    """Every node in the list must be processed."""

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_all_nodes_processed(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        nodes = ["node-1", "node-2", "node-3"]
        mock_get_nodes.return_value = nodes
        mock_k8s.CoreV1Api.return_value.read_node.side_effect = [
            _make_node("i-001"), _make_node("i-002"), _make_node("i-003"),
        ]
        mock_ec2 = MagicMock()
        mock_boto3.return_value = mock_ec2

        upgrade_karpenter_nodes(CLUSTER, REGION)

        assert mock_unschedule.call_count == 3
        assert mock_drain.call_count == 3
        assert mock_ec2.terminate_instances.call_count == 3

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_each_node_terminated_with_correct_instance_id(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        """Each node must be terminated with its own instance ID, not another node's."""
        mock_get_nodes.return_value = ["node-a", "node-b"]
        mock_k8s.CoreV1Api.return_value.read_node.side_effect = [
            _make_node("i-aaa"), _make_node("i-bbb"),
        ]
        mock_ec2 = MagicMock()
        mock_boto3.return_value = mock_ec2

        upgrade_karpenter_nodes(CLUSTER, REGION)

        terminate_calls = mock_ec2.terminate_instances.call_args_list
        assert terminate_calls[0] == call(InstanceIds=["i-aaa"])
        assert terminate_calls[1] == call(InstanceIds=["i-bbb"])

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_cordon_and_drain_called_per_node(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        nodes = ["node-x", "node-y"]
        mock_get_nodes.return_value = nodes
        mock_k8s.CoreV1Api.return_value.read_node.side_effect = [
            _make_node("i-xxx"), _make_node("i-yyy"),
        ]
        mock_boto3.return_value = MagicMock()

        upgrade_karpenter_nodes(CLUSTER, REGION)

        cordon_nodes = [c.kwargs["node_name"] for c in mock_unschedule.call_args_list]
        drain_nodes = [c.kwargs["node_name"] for c in mock_drain.call_args_list]
        assert cordon_nodes == nodes
        assert drain_nodes == nodes


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    """Failures at any step must propagate immediately as exceptions."""

    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_read_node_failure_raises(
        self, mock_loading, mock_get_nodes, mock_boto3, mock_k8s,
    ):
        mock_get_nodes.return_value = ["bad-node"]
        mock_k8s.CoreV1Api.return_value.read_node.side_effect = Exception("node not found")

        with pytest.raises(Exception, match="node not found"):
            upgrade_karpenter_nodes(CLUSTER, REGION)

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_ec2_terminate_failure_raises(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        mock_get_nodes.return_value = ["node-1"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node("i-xxx")
        mock_ec2 = MagicMock()
        mock_ec2.terminate_instances.side_effect = Exception("EC2 termination failed")
        mock_boto3.return_value = mock_ec2

        with pytest.raises(Exception, match="EC2 termination failed"):
            upgrade_karpenter_nodes(CLUSTER, REGION)

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_drain_failure_stops_subsequent_nodes(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        """If drain raises (e.g. PDB violation), the remaining nodes must not be processed."""
        mock_get_nodes.return_value = ["node-1", "node-2"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node("i-xxx")
        mock_drain.side_effect = Exception("PDB violation")
        mock_boto3.return_value = MagicMock()

        with pytest.raises(Exception, match="PDB violation"):
            upgrade_karpenter_nodes(CLUSTER, REGION)

        # node-2 must never have been cordoned
        assert mock_unschedule.call_count == 1

    @patch(_DRAIN)
    @patch(_UNSCHEDULE)
    @patch(_K8S)
    @patch(_BOTO3)
    @patch(_GET_NODES)
    @patch(_LOADING)
    def test_cordon_failure_stops_upgrade(
        self, mock_loading, mock_get_nodes, mock_boto3,
        mock_k8s, mock_unschedule, mock_drain,
    ):
        mock_get_nodes.return_value = ["node-1", "node-2"]
        mock_k8s.CoreV1Api.return_value.read_node.return_value = _make_node("i-xxx")
        mock_unschedule.side_effect = Exception("cordon failed")
        mock_boto3.return_value = MagicMock()

        with pytest.raises(Exception, match="cordon failed"):
            upgrade_karpenter_nodes(CLUSTER, REGION)

        mock_drain.assert_not_called()

"""Test StatefulSet pod readiness wait after self-managed node replacement.

When a self-managed node is rolled, StatefulSet pods evicted from it must be
confirmed Running+Ready on a replacement node before the old instance is
terminated — a pod cannot become Ready until its PVC re-binds, so pod-Ready is
the PVC-rebind confirmation. DaemonSet/other pods are out of scope here.

The StatefulSet pods must be captured BEFORE draining, since drain removes them
from the node.
"""

from unittest.mock import MagicMock, patch

import pytest

from eksupgrade.src.k8s_client import get_statefulset_pods_on_node, wait_for_statefulset_pods_ready

CLUSTER = "test-cluster"
REGION = "us-east-1"
NODE = "ip-10-0-1-1.ec2.internal"

_LOADING = "eksupgrade.src.k8s_client.loading_config"
_K8S = "eksupgrade.src.k8s_client.client"


def _owner(kind: str) -> MagicMock:
    ref = MagicMock()
    ref.kind = kind
    return ref


def _pod(name, owner_kind, node=NODE, ready=False, phase="Running", namespace="default") -> MagicMock:
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.namespace = namespace
    pod.metadata.owner_references = [_owner(owner_kind)] if owner_kind else []
    pod.spec.node_name = node
    pod.status.phase = phase
    cond = MagicMock()
    cond.type = "Ready"
    cond.status = "True" if ready else "False"
    pod.status.conditions = [cond]
    return pod


def _pod_list(*pods) -> MagicMock:
    resp = MagicMock()
    resp.items = list(pods)
    return resp


class TestGetStatefulSetPodsOnNode:
    @patch(_K8S)
    @patch(_LOADING)
    def test_returns_only_statefulset_pods(self, mock_loading, mock_k8s):
        core = mock_k8s.CoreV1Api.return_value
        core.list_pod_for_all_namespaces.return_value = _pod_list(
            _pod("ss-0", "StatefulSet"),
            _pod("web-abc", "ReplicaSet"),
            _pod("ds-x", "DaemonSet"),
        )

        result = get_statefulset_pods_on_node(CLUSTER, NODE, REGION)

        assert result == [("ss-0", "default")]


class TestWaitForStatefulSetPodsReady:
    @patch("eksupgrade.src.k8s_client.time.sleep", return_value=None)
    @patch(_K8S)
    @patch(_LOADING)
    def test_empty_set_returns_immediately(self, mock_loading, mock_k8s, mock_sleep):
        core = mock_k8s.CoreV1Api.return_value
        assert wait_for_statefulset_pods_ready(CLUSTER, REGION, [], timeout=30, poll_interval=5) is True
        core.read_namespaced_pod.assert_not_called()

    @patch("eksupgrade.src.k8s_client.time.sleep", return_value=None)
    @patch(_K8S)
    @patch(_LOADING)
    def test_waits_until_replacement_ready(self, mock_loading, mock_k8s, mock_sleep):
        core = mock_k8s.CoreV1Api.return_value
        # First poll: not ready; second poll: Running+Ready on a new node.
        core.read_namespaced_pod.side_effect = [
            _pod("ss-0", "StatefulSet", node="new-node", ready=False),
            _pod("ss-0", "StatefulSet", node="new-node", ready=True),
        ]

        result = wait_for_statefulset_pods_ready(CLUSTER, REGION, [("ss-0", "default")], timeout=30, poll_interval=5)

        assert result is True

    @patch("eksupgrade.src.k8s_client.time.sleep", return_value=None)
    @patch("eksupgrade.src.k8s_client.time.monotonic")
    @patch(_K8S)
    @patch(_LOADING)
    def test_times_out_without_forcing(self, mock_loading, mock_k8s, mock_monotonic, mock_sleep):
        mock_monotonic.side_effect = [0, 1, 1000]
        core = mock_k8s.CoreV1Api.return_value
        core.read_namespaced_pod.return_value = _pod("ss-0", "StatefulSet", ready=False)

        result = wait_for_statefulset_pods_ready(CLUSTER, REGION, [("ss-0", "default")], timeout=30, poll_interval=5)

        assert result is False

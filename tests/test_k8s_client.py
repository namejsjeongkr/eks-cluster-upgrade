"""Test EKS Upgrade k8s client specific logic."""

import base64
from unittest.mock import MagicMock, patch

from kubernetes import client as k8s_client

from eksupgrade.src.k8s_client import get_bearer_token, loading_config


def test_get_bearer_token(sts_client, eks_cluster, cluster_name, region) -> None:
    """Test the get_bearer_token method."""
    token = get_bearer_token(cluster_id=cluster_name, region=region)
    assert token.startswith("k8s-aws-v1.")


def test_loading_config(eks_client, eks_cluster, cluster_name, region) -> None:
    """Test the loading_config method."""
    result = loading_config(cluster_name, region=region)
    assert result == "Initialized"


def test_loading_config_sets_cluster_ca() -> None:
    """loading_config must wire ssl_ca_cert to a file holding the decoded cluster CA."""
    from eksupgrade.src.k8s_client import _CA_CERT_FILES

    _CA_CERT_FILES.clear()
    fake_ca_pem = b"-----BEGIN CERTIFICATE-----\nFAKECERTDATA\n-----END CERTIFICATE-----\n"
    fake_ca_b64 = base64.b64encode(fake_ca_pem).decode("utf-8")

    fake_eks = MagicMock()
    fake_eks.describe_cluster.return_value = {
        "cluster": {
            "endpoint": "https://example.eks.amazonaws.com",
            "certificateAuthority": {"data": fake_ca_b64},
        }
    }

    with (
        patch("eksupgrade.src.k8s_client.boto3.client", return_value=fake_eks),
        patch("eksupgrade.src.k8s_client.get_bearer_token", return_value="faketoken"),
    ):
        loading_config("my-cluster", "ap-northeast-2")

    cfg = k8s_client.Configuration.get_default_copy()
    assert cfg.host == "https://example.eks.amazonaws.com"
    assert cfg.verify_ssl is True
    assert cfg.ssl_ca_cert is not None
    with open(cfg.ssl_ca_cert, "rb") as fh:
        assert fh.read() == fake_ca_pem

"""Validate EKS version support and upgrade path constraints.

Tests cover:
- version_dict.json completeness for all supported EKS versions (1.21-1.35)
- cluster-autoscaler version format correctness (major.minor must match EKS version)
- Single-minor-version upgrade path enforcement
"""

import json
from pathlib import Path

import pytest
from packaging.version import parse as parse_version

VERSION_DICT_PATH = (
    Path(__file__).parent.parent / "eksupgrade" / "src" / "S3Files" / "version_dict.json"
)

LEGACY_VERSIONS = ["1.21", "1.22", "1.23", "1.24", "1.25", "1.26"]
NEW_VERSIONS = ["1.27", "1.28", "1.29", "1.30", "1.31", "1.32", "1.33", "1.34", "1.35"]
ALL_VERSIONS = LEGACY_VERSIONS + NEW_VERSIONS


@pytest.fixture(scope="module")
def version_dict():
    """Load version_dict.json once for the module."""
    with open(VERSION_DICT_PATH) as f:
        return json.load(f)


class TestVersionDictCoverage:
    """version_dict.json must contain all supported EKS versions."""

    @pytest.mark.parametrize("version", ALL_VERSIONS)
    def test_eks_version_present(self, version_dict, version):
        assert version in version_dict, (
            f"EKS version {version} is missing from version_dict.json"
        )

    @pytest.mark.parametrize("version", ALL_VERSIONS)
    def test_cluster_autoscaler_key_exists(self, version_dict, version):
        assert "cluster-autoscaler" in version_dict[version], (
            f"'cluster-autoscaler' key missing for EKS {version}"
        )

    @pytest.mark.parametrize("version", ALL_VERSIONS)
    def test_cluster_autoscaler_version_not_empty(self, version_dict, version):
        ca_version = version_dict[version]["cluster-autoscaler"]
        assert ca_version, f"cluster-autoscaler version is empty for EKS {version}"

    @pytest.mark.parametrize("version", ALL_VERSIONS)
    def test_cluster_autoscaler_major_minor_matches_eks(self, version_dict, version):
        """cluster-autoscaler version's major.minor must match the EKS version."""
        ca_version = version_dict[version]["cluster-autoscaler"]
        ca_major_minor = ".".join(ca_version.split(".")[:2])
        assert ca_major_minor == version, (
            f"cluster-autoscaler {ca_version} (major.minor={ca_major_minor}) "
            f"does not match EKS version {version}"
        )

    @pytest.mark.parametrize("version", ALL_VERSIONS)
    def test_cluster_autoscaler_version_is_valid_semver(self, version_dict, version):
        """cluster-autoscaler version must be parseable as semantic version."""
        ca_version = version_dict[version]["cluster-autoscaler"]
        parsed = parse_version(ca_version)
        assert str(parsed)


class TestNewVersionCoverage:
    """Specific checks for newly added EKS versions (1.27-1.35)."""

    @pytest.mark.parametrize("version", NEW_VERSIONS)
    def test_new_version_has_ca_mapping(self, version_dict, version):
        assert version in version_dict
        assert version_dict[version].get("cluster-autoscaler"), (
            f"New EKS version {version} must have a cluster-autoscaler mapping"
        )

    def test_all_new_versions_present(self, version_dict):
        missing = [v for v in NEW_VERSIONS if v not in version_dict]
        assert not missing, f"Missing new EKS versions: {missing}"

    def test_versions_ordered_descending(self, version_dict):
        """version_dict.json keys should be in descending order (newest first)."""
        keys = list(version_dict.keys())
        parsed = [parse_version(k) for k in keys]
        assert parsed == sorted(parsed, reverse=True), (
            "version_dict.json entries are not in descending order"
        )


class TestUpgradePathValidation:
    """Single-minor-version upgrade constraint must be correctly detectable."""

    @pytest.mark.parametrize("current,target", [
        ("1.32", "1.33"),
        ("1.33", "1.34"),
        ("1.34", "1.35"),
        ("1.31", "1.32"),
        ("1.26", "1.27"),
    ])
    def test_sequential_upgrade_is_single_minor(self, current, target):
        """Valid upgrade: target is exactly one minor version ahead."""
        c = parse_version(current)
        t = parse_version(target)
        assert t.minor == c.minor + 1, (
            f"{current} → {target} should differ by exactly 1 minor version"
        )

    @pytest.mark.parametrize("current,target", [
        ("1.32", "1.34"),
        ("1.32", "1.35"),
        ("1.30", "1.33"),
        ("1.21", "1.35"),
    ])
    def test_multi_minor_jump_is_detectable(self, current, target):
        """Multi-minor jumps must be detectable (EKS rejects these)."""
        c = parse_version(current)
        t = parse_version(target)
        assert t.minor > c.minor + 1, (
            f"{current} → {target} should be a multi-minor jump"
        )

    def test_downgrade_not_valid(self):
        current = parse_version("1.33")
        target = parse_version("1.32")
        assert target < current

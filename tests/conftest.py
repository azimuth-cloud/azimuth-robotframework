"""Provide shared fixtures for the Azimuth library tests."""

import dataclasses
import types
from typing import Any

import pytest

from Azimuth.kubernetes_clusters import KubernetesClusterKeywords


@dataclasses.dataclass
class FakeResource:
    """Serve instances of one Azimuth resource from memory, keyed by ID."""

    instances: dict[str, dict[str, Any]] = dataclasses.field(default_factory=dict)

    def fetch(self, id: str) -> dict[str, Any]:  # noqa: A002
        """Return the stored instance, failing like the API for unknown IDs."""
        return self.instances[id]


@dataclasses.dataclass
class FakeAzimuthClient:
    """Stand in for the Azimuth SDK client."""

    clusters: FakeResource = dataclasses.field(default_factory=FakeResource)

    def kubernetes_clusters(self) -> FakeResource:
        """Return the fake Kubernetes clusters resource."""
        return self.clusters


@pytest.fixture
def azimuth_client() -> FakeAzimuthClient:
    """Provide a fake Azimuth SDK client with no resources."""
    return FakeAzimuthClient()


@pytest.fixture
def kubernetes_cluster_keywords(
    azimuth_client: FakeAzimuthClient,
) -> KubernetesClusterKeywords:
    """Provide the Kubernetes cluster keywords backed by the fake client."""
    return KubernetesClusterKeywords(types.SimpleNamespace(client=azimuth_client))

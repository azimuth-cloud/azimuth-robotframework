"""Test the Kubernetes cluster keywords."""

from Azimuth.kubernetes_clusters import KubernetesClusterKeywords

from .conftest import FakeAzimuthClient


def test_get_service_url_uses_cluster_id(
    azimuth_client: FakeAzimuthClient,
    kubernetes_cluster_keywords: KubernetesClusterKeywords,
) -> None:
    """Check that the service URL is looked up using the cluster's "id" field."""
    cluster = {
        "id": "cluster-1",
        "name": "test",
        "services": [
            {"name": "kubernetes-dashboard-client", "fqdn": "dashboard.example.com"},
        ],
    }
    azimuth_client.clusters.instances["cluster-1"] = cluster

    url = kubernetes_cluster_keywords.get_kubernetes_cluster_service_url(
        cluster, "dashboard"
    )

    assert url == "dashboard.example.com"

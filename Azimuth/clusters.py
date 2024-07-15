import typing as t

from robot.api.deco import keyword

from . import util


class ClusterKeywords:
    """
    Keywords for interacting with clusters.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.clusters()

    @keyword
    def list_clusters(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists clusters using the active client.
        """
        return list(self._resource.list())
    
    @keyword
    def fetch_cluster_by_id(self, id: str) -> t.Dict[str, t.Any]:
        """
        Fetches a cluster by id using the active client.
        """
        return self._resource.fetch(id)
    
    @keyword
    def find_cluster_by_name(self, name: str) -> t.Dict[str, t.Any]:
        """
        Finds a cluster by name using the active client.
        """
        try:
            return next(c for c in self._resource.list() if c.name == name)
        except StopIteration:
            raise ValueError(f"no cluster with name '{name}'")
    
    @keyword
    def create_cluster(
        self,
        name: str,
        cluster_type: str,
        **parameter_values: t.Any
    ) -> t.Dict[str, t.Any]:
        """
        Creates a cluster using the active client.
        """
        return self._resource.create({
            "name": name,
            "cluster_type" : cluster_type,
            "parameter_values": parameter_values,
        })

    @keyword
    def patch_cluster(self, id: str) -> t.Dict[str, t.Any]:
        """
        Patches the specified cluster.
        """
        return self._resource.action(id, "patch")

    @keyword
    def delete_cluster(self, id: str, interval: int = 15):
        """
        Deletes the specified cluster and waits for it to be deleted.
        """
        util.delete_resource(self._resource, id, interval)

    @keyword
    def wait_for_cluster_status(
        self,
        id: str,
        target_status: str,
        interval: int = 15
    ) -> t.Dict[str, t.Any]:
        """
        Waits for the specified cluster to reach the target status before returning it.
        """
        return util.wait_for_resource_property(
            self._resource,
            id,
            "status",
            target_status,
            {"CONFIGURING", "DELETING"},
            "error_message",
            interval
        )

    @keyword
    def wait_for_cluster_ready(self, id: str, interval: int = 15) -> t.Dict[str, t.Any]:
        """
        Waits for the cluster status to be ready before returning it.
        """
        return self.wait_for_cluster_status(id, "READY", interval)

    @keyword
    def get_cluster_service_url(self, cluster: t.Dict[str, t.Any], name: str) -> str:
        """
        Returns the Zenith FQDN for the specified cluster service.
        """
        try:
            return next(
                service["fqdn"]
                for service in cluster["services"]
                if service["name"] == name
            )
        except StopIteration:
            raise ValueError(f"no such service - {name}")

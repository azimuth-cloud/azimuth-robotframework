import typing as t

from robot.api.deco import keyword

from . import util


class ClusterServiceDict(t.TypedDict):
    """
    Represents a service for a cluster.
    """
    name: str
    label: str
    icon_url: t.Optional[str]
    fqdn: str
    subdomain: str
    url: str


class ClusterDict(t.TypedDict):
    """
    Represents a cluster resource returned from the Azimuth API.
    """
    id: str
    name: str
    cluster_type: str
    status: str
    task: t.Optional[str]
    error_message: t.Optional[str]
    parameter_values: t.Dict[str, t.Any]
    tags: t.List[str]
    outputs: t.Dict[str, t.Any]
    created: str
    updated: str
    patched: str
    services: t.List[ClusterServiceDict]
    links: t.Dict[str, str]


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
    def list_clusters(self) -> t.List[ClusterDict]:
        """
        Lists clusters using the active client.
        """
        return list(self._resource.list())
    
    @keyword
    def fetch_cluster_by_id(self, id: str) -> ClusterDict:
        """
        Fetches a cluster by id using the active client.
        """
        return self._resource.fetch(id)
    
    @keyword
    def find_cluster_by_name(self, name: str) -> ClusterDict:
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
    ) -> ClusterDict:
        """
        Creates a cluster using the active client.
        """
        return self._resource.create({
            "name": name,
            "cluster_type" : cluster_type,
            "parameter_values": parameter_values,
        })
    
    @keyword
    def update_cluster(self, id: str, parameter_values: t.Dict[str, t.Any]) -> ClusterDict:
        """
        Updates the specified cluster with the given parameter values using the active client.
        """
        return self._resource.patch(id, { "parameter_values": parameter_values })

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
    ) -> ClusterDict:
        """
        Waits for the specified cluster to reach the target status before returning it.

        If the cluster has not reached that state within the timeout, the keyword fails.
        """
        return util.wait_for_resource_property(
            self._resource,
            id,
            "status",
            target_status,
            {"CONFIGURING", "DELETING"},
            interval
        )

    @keyword
    def wait_for_cluster_ready(self, id: str, interval: int = 15) -> ClusterDict:
        """
        Waits for the cluster status to be ready before returning it.
        """
        return self.wait_for_cluster_status(id, "READY", interval)

    @keyword
    def get_cluster_service_url(self, cluster: ClusterDict, name: str) -> str:
        """
        Returns the Zenith FQDN for the specified service.
        """
        try:
            return next(
                service["fqdn"]
                for service in cluster["services"]
                if service["name"] == name
            )
        except StopIteration:
            raise ValueError(f"no such service - {name}")

import typing as t

from robot.api.deco import keyword


class ClusterTypeKeywords:
    """
    Keywords for interacting with cluster types.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.cluster_types()

    @keyword
    def list_cluster_types(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists cluster types using the active client.
        """
        return list(self._resource.list())
    
    @keyword
    def fetch_cluster_type(self, name: str) -> t.Dict[str, t.Any]:
        """
        Fetches a cluster type by name using the active client.
        """
        return self._resource.fetch(name)

    @keyword
    def find_cluster_type_by_name(self, name: str) -> t.Dict[str, t.Any]:
        """
        Fetches a cluster type by name using the active client.
        """
        return self.fetch_cluster_type(name)

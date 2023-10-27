import typing as t

from robot.api import logger
from robot.api.deco import keyword

from .external_ips import ExternalIpKeywords
from .sizes import SizeKeywords


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

    @keyword
    def get_defaults_for_cluster_type(
        self,
        cluster_type: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Returns the default parameters for a cluster type.
        """
        return {
            param["name"]: param["default"]
            for param in cluster_type["parameters"]
            if param.get("default") is not None
        }

    def _guess_size(self, param):
        # We want to find the smallest size that fulfils the constraints
        # We sort by RAM, then CPUs, then disk to find the smallest
        options = param.get("options", {})
        try:
            size = SizeKeywords(self._ctx).find_smallest_size_with_resources(**options)
        except ValueError:
            return None
        else:
            return size.id

    def _guess_ip(self, param):
        return ExternalIpKeywords(self._ctx).find_free_or_allocate_external_ip().id

    @keyword
    def guess_parameter_values_for_cluster_type(
        self,
        cluster_type: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Attempts to guess suitable parameter values for a cluster type based on the
        parameter types and constraints.
        """
        params = {}
        # Try to fill in the remaining parameters by guessing
        for param in cluster_type["parameters"]:
            name = param["name"]
            guess = None
            # We don't currently handle all the types
            if param.get("default") is not None:
                guess = param["default"]
            elif param["kind"] == "cloud.size":
                guess = self._guess_size(param)
            elif param["kind"] == "cloud.ip":
                guess = self._guess_ip(param)
            # Emit a warning if we weren't able to make a guess
            if guess is not None:
                params[name] = guess
        return params

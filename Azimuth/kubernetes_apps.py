import typing as t

from robot.api.deco import keyword

from . import util


class KubernetesAppKeywords:
    """
    Keywords for interacting with Kubernetes apps.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.kubernetes_apps()

    @keyword
    def list_kubernetes_apps(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists Kubernetes apps using the active client.
        """
        return list(self._resource.list())

    @keyword
    def fetch_kubernetes_app_by_id(self, id: str) -> t.Dict[str, t.Any]:
        """
        Fetches a Kubernetes app by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def find_kubernetes_app_by_name(self, name: str) -> t.Dict[str, t.Any]:
        """
        Finds a Kubernetes app by name using the active client.
        """
        try:
            return next(c for c in self._resource.list() if c.name == name)
        except StopIteration:
            raise ValueError(f"no Kubernetes app with name '{name}'")

    @keyword
    def create_kubernetes_app(
        self,
        name: str,
        template: str,
        kubernetes_cluster: str,
        values: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Creates a Kubernetes app using the active client.
        """
        return self._resource.create({
            "name": name,
            "template": template,
            "kubernetes_cluster": kubernetes_cluster,
            "values": values,
        })

    @keyword
    def update_kubernetes_app(
        self,
        id: str,
        version: t.Dict[str, t.Any],
        values: t.Dict[str, t.Any]
    ) -> t.Dict[str, t.Any]:
        """
        Update the specified Kubernetes app with the given version and values.
        """
        return self._resource.patch(id, {"version": version["name"], "values": values})

    @keyword
    def delete_kubernetes_app(self, id: str, interval: int = 15):
        """
        Deletes the specified Kubernetes app and waits for it to be deleted.
        """
        util.delete_resource(self._resource, id, interval)

    @keyword
    def wait_for_kubernetes_app_status(
        self,
        id: str,
        target_status: str,
        interval: int = 15
    ) -> t.Dict[str, t.Any]:
        """
        Waits for the specified app to reach the target status before returning it.
        """
        return util.wait_for_resource_property(
            self._resource,
            id,
            "status",
            target_status,
            {"Pending", "Preparing", "Installing", "Upgrading", "Uninstalling", "Unknown"},
            "error_message",
            interval
        )

    @keyword
    def wait_for_kubernetes_app_deployed(self, id: str, interval: int = 15) -> t.Dict[str, t.Any]:
        """
        Waits for the app status to be deployed before returning it.
        """
        return self.wait_for_kubernetes_app_status(id, "Deployed", interval)

    @keyword
    def wait_for_kubernetes_app_service_url(self, id: str, name: str, interval: int = 15) -> str:
        """
        Returns the Zenith FQDN for the specified app service.

        Because Helm does not know what constitutes a healthy Zenith reservation/client,
        this may involve a wait.
        """
        # We also consider the given name with the app id prefixed, as that is how the names are
        # constructed in the Helm releases of the reference charts
        names = {name, f"{id}-{name}"}
        app = util.wait_for_resource(
            self._resource,
            id,
            lambda app: any(s["name"] in names for s in app["services"]),
            interval
        )
        return next(s["fqdn"] for s in app["services"] if s["name"] in names)

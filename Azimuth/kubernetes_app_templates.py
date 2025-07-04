import typing as t

import jsonschema_default
from robot.api.deco import keyword


class KubernetesAppTemplateKeywords:
    """
    Keywords for interacting with Kubernetes app templates.
    """

    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.kubernetes_app_templates()

    @keyword
    def list_kubernetes_app_templates(self) -> list[dict[str, t.Any]]:
        """
        Lists available Kubernetes app templates using the active client.
        """
        return list(self._resource.list())

    @keyword
    def fetch_kubernetes_app_template(self, id: str) -> dict[str, t.Any]:  # noqa: A002
        """
        Fetches a Kubernetes app template by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def get_latest_version_for_kubernetes_app_template(
        self, template: dict[str, t.Any]
    ) -> dict[str, t.Any]:
        """
        Returns the latest version for the Kubernetes app template.
        """
        try:
            return next(iter(template["versions"]))
        except StopIteration:
            raise AssertionError(
                f"no versions for Kubernetes app template '{template.id}'"
            )

    @keyword
    def get_defaults_for_kubernetes_app_template_version(
        self, version: dict[str, t.Any]
    ) -> dict[str, t.Any]:
        """
        Gets the default values for the given Kubernetes app template.
        """
        return jsonschema_default.create_from(version["values_schema"])

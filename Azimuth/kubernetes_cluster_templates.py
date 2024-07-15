import typing as t

from robot.api.deco import keyword

import easysemver


class KubernetesClusterTemplateKeywords:
    """
    Keywords for interacting with Kubernetes cluster templates.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.kubernetes_cluster_templates()

    @keyword
    def list_kubernetes_cluster_templates(
        self,
        *,
        tags: t.Optional[t.List[str]] = None,
        include_deprecated: bool = True
    ) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists available Kubernetes cluster templates using the active client.
        """
        return list(
            template
            for template in self._resource.list()
            if (
                set(tags or []).issubset(template.tags) and
                (include_deprecated or not template.deprecated)
            )
        )

    @keyword
    def fetch_kubernetes_cluster_template(self, id: str) -> t.Dict[str, t.Any]:
        """
        Fetches a Kubernetes cluster template by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def find_kubernetes_cluster_template_by_name(self, name: str) -> t.Dict[str, t.Any]:
        """
        Finds a Kubernetes cluster template by name using the active client.
        """
        try:
            return next(ct for ct in self._resource.list() if ct.name == name)
        except StopIteration:
            raise ValueError(f"no template with name '{name}'")

    @keyword
    def find_latest_kubernetes_cluster_template(
        self,
        *,
        constraints: str = ">=0.0.0",
        tags: t.Optional[t.List[str]] = None,
        include_deprecated: bool = False
    ) -> t.Dict[str, t.Any]:
        """
        Finds the Kubernetes template with the most recent version that matches the constraints.
        """
        templates = self.list_kubernetes_cluster_templates(
            tags = tags,
            include_deprecated = include_deprecated
        )
        version_range = easysemver.Range(constraints)
        latest_version = None
        latest_template = None
        for template in templates:
            current_version = easysemver.Version(template.kubernetes_version)
            # The version must match the constraints
            if current_version not in version_range:
                continue
            # The version must beat the one we already have
            if latest_version and latest_version >= current_version:
                continue
            latest_version = current_version
            latest_template = template
        if not latest_template:
            raise ValueError("no template matching conditions")
        return latest_template

    @keyword
    def find_kubernetes_cluster_template_for_upgrade(self, id: str) -> t.Dict[str, t.Any]:
        """
        Given the ID of an existing template, find the latest template that a cluster using
        that template can be upgraded to.
        """
        template = self.fetch_kubernetes_cluster_template(id)
        # The Kubernetes version for the new template must be the same or newer than the
        # given template and no more than one minor version newer than the given template
        # We also restrict the search to non-deprecated templates with the same tags
        current_version = easysemver.Version(template.kubernetes_version)
        upper_bound = current_version.bump_minor().bump_minor()
        return self.find_latest_kubernetes_cluster_template(
            constraints = f">={current_version},<{upper_bound}",
            tags = template.get("tags", []),
            include_deprecated = False
        )

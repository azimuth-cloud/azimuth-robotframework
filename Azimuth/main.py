import secrets
import string
import typing as t

from robotlibcore import DynamicCore, keyword

from azimuth_sdk import Configuration, SyncClient

from .cluster_types import ClusterTypeKeywords
from .clusters import ClusterKeywords
from .external_ips import ExternalIpKeywords
from .kubernetes_app_templates import KubernetesAppTemplateKeywords
from .kubernetes_apps import KubernetesAppKeywords
from .kubernetes_cluster_templates import KubernetesClusterTemplateKeywords
from .kubernetes_clusters import KubernetesClusterKeywords
from .sizes import SizeKeywords
from .zenith import ZenithKeywords


class Azimuth(DynamicCore):
    """
    Robot Framework library for testing Azimuth.
    """
    # This ensures that one instance of the library is shared everywhere
    # This is important to get the client sharing working correctly
    ROBOT_LIBRARY_SCOPE = "GLOBAL"
    ROBOT_LISTENER_API_VERSION = 3

    def __init__(self):
        # The Azimuth client instance
        self.client: SyncClient = None
        self.ROBOT_LIBRARY_LISTENER = self
        super().__init__([
            ClusterTypeKeywords(self),
            ClusterKeywords(self),
            ExternalIpKeywords(self),
            KubernetesAppTemplateKeywords(self),
            KubernetesAppKeywords(self),
            KubernetesClusterTemplateKeywords(self),
            KubernetesClusterKeywords(self),
            SizeKeywords(self),
            ZenithKeywords(self),
        ])

    def _create_client(self, config):
        # Clean up any old clients first
        if self.client:
            self.client.__exit__()
        self.client = config.sync_client()
        self.client.__enter__()

    @keyword
    def create_client(
        self,
        base_url: str,
        *,
        auth_data: t.Dict[str, t.Any],
        authenticator: t.Optional[str] = None,
        authenticator_type: t.Optional[str] = None,
        default_tenancy_id: t.Optional[str] = None,
        **kwargs
    ):
        """
        Creates an Azimuth SDK client using the given authentication.
        """
        self._create_client(
            Configuration.create(
                base_url,
                auth_data = auth_data,
                authenticator = authenticator,
                authenticator_type = authenticator_type,
                default_tenancy_id = default_tenancy_id,
                **kwargs
            )
        )

    @keyword
    def create_client_from_openstack_clouds_file(
        self,
        base_url: str,
        path: str,
        cloud: str = "openstack",
        *,
        default_tenancy_id: t.Optional[str] = None,
        **kwargs
    ):
        """
        Creates an Azimuth SDK client using the specified OpenStack clouds.yaml file.
        """
        self._create_client(
            Configuration.from_openstack_clouds_file(
                base_url,
                path,
                cloud = cloud,
                default_tenancy_id = default_tenancy_id,
                **kwargs
            )
        )

    @keyword
    def create_client_from_environment(
        self,
        base_url: str,
        *,
        default_tenancy_id: t.Optional[str] = None,
        **kwargs
    ):
        """
        Creates an Azimuth SDK client from the environment variables.
        """
        self._create_client(
            Configuration.from_environment(
                base_url,
                default_tenancy_id = default_tenancy_id,
                **kwargs
            )
        )

    @keyword
    def switch_tenancy(self, tenancy_id: str):
        """
        Switches the current client to a different tenancy.
        """
        assert self.client is not None
        self.client.switch_tenancy(tenancy_id)

    @keyword
    def close_client(self):
        """
        Closes the active client.
        """
        if self.client:
            self.client.__exit__()
            self.client = None

    @keyword
    def generate_name(self, prefix: str, suffix_length = 5, suffix_chars = None) -> str:
        """
        Given a prefix, generates a name with a random suffix.
        """
        suffix_chars = suffix_chars or (string.ascii_lowercase + string.digits)
        suffix = "".join(secrets.choice(suffix_chars) for _ in range(suffix_length))
        return f"{prefix}-{suffix}"

    def _end_test(self, data, result):
        mins, secs = divmod(result.elapsedtime / 1000, 60)
        print(f"elapsed time: {mins:0>1.0f}m {secs:.0f}s")

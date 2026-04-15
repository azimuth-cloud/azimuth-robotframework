import contextlib
import dataclasses
import enum
import io
import os
import subprocess
import tarfile
import tempfile
import time
import typing as t

from robot.api import logger
from robot.api.deco import keyword

from . import util


@dataclasses.dataclass(frozen=True)
class NodeGroupConfig:
    """
    Type for a node group config.
    """

    name: str
    machine_size: str
    autoscale: bool = False
    count: int | None = None
    min_count: int | None = None
    max_count: int | None = None

    def __post_init__(self):
        if self.autoscale:
            assert (
                self.min_count is not None and self.max_count is not None
            ), "min_count and max_count are required for autoscaling groups"
        else:
            assert (
                self.count is not None
            ), "count is required for non-autoscaling groups"


@dataclasses.dataclass(frozen=True)
class KubernetesClusterConfig:
    """
    Type for a Kubernetes cluster config.
    """

    name: str
    template: str
    control_plane_size: str
    node_groups: list[NodeGroupConfig] = dataclasses.field(default_factory=list)
    autohealing_enabled: bool = True
    dashboard_enabled: bool = False
    ingress_enabled: bool = False
    ingress_controller_load_balancer_ip: str | None = None
    monitoring_enabled: bool = False


@enum.unique
class SonobuoyMode(enum.Enum):
    """
    Enumeration of Sonobuoy modes.
    """

    CERTIFIED_CONFORMANCE = "certified-conformance"
    CONFORMANCE_LITE = "conformance-lite"
    NON_DISRUPTIVE_CONFORMANCE = "non-disruptive-conformance"
    QUICK = "quick"


class KubernetesClusterKeywords:
    """
    Keywords for interacting with Kubernetes clusters.
    """

    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.kubernetes_clusters()

    @keyword
    def list_kubernetes_clusters(self) -> list[dict[str, t.Any]]:
        """
        Lists Kubernetes clusters using the active client.
        """
        return list(self._resource.list())

    @keyword
    def fetch_kubernetes_cluster_by_id(self, id: str) -> dict[str, t.Any]:  # noqa: A002
        """
        Fetches a Kubernetes cluster by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def find_kubernetes_cluster_by_name(self, name: str) -> dict[str, t.Any]:
        """
        Finds a Kubernetes cluster by name using the active client.
        """
        try:
            return next(c for c in self._resource.list() if c.name == name)
        except StopIteration:
            raise ValueError(f"no Kubernetes cluster with name '{name}'")

    @keyword
    def new_kubernetes_config(
        self, *, name: str, template: str, control_plane_size: str
    ) -> KubernetesClusterConfig:
        """
        Initialises a new Kubernetes config.
        """
        return KubernetesClusterConfig(name, template, control_plane_size)

    @keyword
    def change_template_for_kubernetes_config(
        self, config: KubernetesClusterConfig, *, template: str
    ) -> KubernetesClusterConfig:
        """
        Changes the template for the current Kubernetes config.
        """
        return dataclasses.replace(config, template=template)

    @keyword
    def add_node_group_to_kubernetes_config(
        self,
        config: KubernetesClusterConfig,
        *,
        name: str,
        machine_size: str,
        autoscale: bool = False,
        count: int | None = None,
        min_count: int | None = None,
        max_count: int | None = None,
    ) -> KubernetesClusterConfig:
        """
        Adds a node group to the current Kubernetes config.
        """
        return dataclasses.replace(
            config,
            node_groups=[
                *config.node_groups,
                NodeGroupConfig(
                    name, machine_size, autoscale, count, min_count, max_count
                ),
            ],
        )

    @keyword
    def enable_autohealing_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Enables auto-healing for the current Kubernetes config.
        """
        return dataclasses.replace(config, autohealing_enabled=True)

    @keyword
    def disable_autohealing_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Disables auto-healing for the current Kubernetes config.
        """
        return dataclasses.replace(config, autohealing_enabled=False)

    @keyword
    def enable_dashboard_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Enables the Kubernetes dashboard for the current Kubernetes config.
        """
        return dataclasses.replace(config, dashboard_enabled=True)

    @keyword
    def disable_dashboard_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Disables the Kubernetes dashboard for the current Kubernetes config.
        """
        return dataclasses.replace(config, dashboard_enabled=False)

    @keyword
    def enable_ingress_for_kubernetes_config(
        self, config: KubernetesClusterConfig, ip: str
    ) -> KubernetesClusterConfig:
        """
        Enables ingress for the current Kubernetes config.
        """
        return dataclasses.replace(
            config, ingress_enabled=True, ingress_controller_load_balancer_ip=ip
        )

    @keyword
    def disable_ingress_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Disables ingress for the current Kubernetes config.
        """
        return dataclasses.replace(
            config, ingress_enabled=False, ingress_controller_load_balancer_ip=None
        )

    @keyword
    def enable_monitoring_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Enables monitoring for the current Kubernetes config.
        """
        return dataclasses.replace(config, monitoring_enabled=True)

    @keyword
    def disable_monitoring_for_kubernetes_config(
        self, config: KubernetesClusterConfig
    ) -> KubernetesClusterConfig:
        """
        Disables monitoring for the current Kubernetes config.
        """
        return dataclasses.replace(config, monitoring_enabled=False)

    @keyword
    def create_kubernetes_cluster(
        self, config: KubernetesClusterConfig
    ) -> dict[str, t.Any]:
        """
        Creates a Kubernetes cluster using the active client.
        """
        return self._resource.create(dataclasses.asdict(config))

    @keyword
    def upgrade_kubernetes_cluster(
        self,
        id: str,  # noqa: A002
        template_id: str,
    ) -> dict[str, t.Any]:
        """
        Upgrades the specified Kubernetes cluster to a new template.
        """
        return self._resource.patch(id, {"template": template_id})

    @keyword
    def delete_kubernetes_cluster(self, id: str, interval: int = 15):  # noqa: A002
        """
        Deletes the specified Kubernetes cluster and waits for it to be deleted.
        """
        util.delete_resource(self._resource, id, interval)

    @keyword
    def get_kubeconfig_for_kubernetes_cluster(self, id: str) -> str:  # noqa: A002
        """
        Returns the kubeconfig for the specified Kubernetes cluster.
        """
        data = self._resource.action(id, "kubeconfig")
        return data["kubeconfig"]

    @keyword
    def wait_for_kubernetes_cluster_status(
        self,
        id: str,  # noqa: A002
        target_status: str,
        interval: int = 15,
    ) -> dict[str, t.Any]:
        """
        Waits for the specified cluster to reach the target status before returning it.
        """
        return util.wait_for_resource_property(
            self._resource,
            id,
            "status",
            target_status,
            {"Pending", "Reconciling", "Upgrading", "Deleting", "Unhealthy", "Unknown"},
            "error_message",
            interval,
        )

    @keyword
    def wait_for_kubernetes_cluster_ready(
        self,
        id: str,  # noqa: A002
        interval: int = 15,
    ) -> dict[str, t.Any]:
        """
        Waits for the cluster status to be ready before returning it.
        """
        return self.wait_for_kubernetes_cluster_status(id, "Ready", interval)

    @keyword
    def get_kubernetes_cluster_service_url(
        self, cluster: dict[str, t.Any], name: str
    ) -> str:
        """
        Returns the Zenith FQDN for the specified cluster service.
        """
        return self.wait_for_kubernetes_cluster_service_url(cluster["id"], name)

    @keyword
    def wait_for_kubernetes_cluster_service_url(
        self,
        id: str,  # noqa: A002
        name: str,
        interval: int = 15,
    ) -> str:
        """
        Returns the Zenith FQDN for the specified cluster service.

        Because the Zenith operator is asynchronous, we wait to see if the services
        appear.
        """
        # Allow some shortcut names
        names = {name}
        if name in {"dashboard", "kubernetes-dashboard"}:
            names.add("kubernetes-dashboard-client")
        if name == "monitoring":
            names.add("monitoring-system-kube-prometheus-stack-client")
        cluster = util.wait_for_resource(
            self._resource,
            id,
            lambda cluster: any(s["name"] in names for s in cluster["services"]),
            interval,
        )
        return next(s["fqdn"] for s in cluster["services"] if s["name"] in names)

    @contextlib.contextmanager
    def _kubeconfig_for_cluster(self, id: str):  # noqa: A002
        kubeconfig = self.get_kubeconfig_for_kubernetes_cluster(id)
        with tempfile.NamedTemporaryFile("w") as file:
            file.write(kubeconfig)
            file.flush()
            yield file.name

    def _run_sonobuoy_cmd(self, executable, cmd, *args):
        proc = subprocess.run([executable, cmd, *args], capture_output=True)
        if proc.returncode != 0:
            logger.info(f"sonobuoy {cmd} command failed")
            logger.info(proc.stderr)
        return proc

    @keyword
    def run_sonobuoy_for_kubernetes_cluster(
        self,
        id: str,  # noqa: A002
        *,
        executable="sonobuoy",
        mode: SonobuoyMode = SonobuoyMode.QUICK,
        run_extra_args: list[str] | None = None,
        results_extra_args: list[str] | None = None,
        delete_extra_args: list[str] | None = None,
    ):
        """
        Runs Sonobuoy conformance tests for the specified cluster.
        """
        with self._kubeconfig_for_cluster(id) as kubeconfig:
            run_proc = self._run_sonobuoy_cmd(
                executable,
                "run",
                "--kubeconfig",
                kubeconfig,
                "--wait",
                "--mode",
                mode.value,
                *(run_extra_args or []),
            )
            # Even if the run failed, we still want to run the retrieve
            # The retrieve command has a known failure mode that is solved by retrying
            while True:
                retrieve_proc = self._run_sonobuoy_cmd(
                    executable,
                    "retrieve",
                    "--kubeconfig",
                    kubeconfig,
                )
                if (
                    retrieve_proc.returncode == 0
                    or "unexpected EOF" not in retrieve_proc.stderr.decode()
                ):
                    break
            # Even if the run and/or retrieve failed, we delete the run
            delete_proc = self._run_sonobuoy_cmd(
                executable,
                "delete",
                "--kubeconfig",
                kubeconfig,
                "--wait",
                *(delete_extra_args or []),
            )

            # If anything failed so far, we are done
            assert run_proc.returncode == 0, "sonobuoy run command failed"
            assert retrieve_proc.returncode == 0, "sonobuoy retrieve command failed"
            assert delete_proc.returncode == 0, "sonobuoy delete command failed"

        # Process and return the results
        results_file = retrieve_proc.stdout.strip()
        results_proc = self._run_sonobuoy_cmd(
            executable, "results", results_file, *(results_extra_args or [])
        )
        os.remove(results_file)
        assert results_proc.returncode == 0, "sonobuoy results command failed"
        logger.info(results_proc.stdout)
        return results_proc.stdout

    def _run_logcli_cmd(self, executable, *args):
        proc = subprocess.run([executable, *args], capture_output=True)
        if proc.returncode != 0:
            logger.info("logcli command failed")
            logger.info(proc.stderr)
        return proc

    @contextlib.contextmanager
    def _port_forward_loki(
        self,
        kubeconfig,
        local_port=3100,
        namespace="monitoring-system",
        service="svc/loki-stack",
    ):
        """
        Starts a kubectl port-forward to the Loki pod and return the local address.
        """
        pf_proc = subprocess.Popen(
            [
                "kubectl",
                "--kubeconfig",
                kubeconfig,
                "port-forward",
                "-n",
                namespace,
                service,
                f"{local_port}:3100",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            time.sleep(2)
            if pf_proc.poll() is not None:
                raise RuntimeError(
                    f"kubectl port-forward exited early: {pf_proc.stderr.read()}"
                )
            yield f"http://localhost:{local_port}"
        finally:
            pf_proc.terminate()
            pf_proc.wait()

    @keyword
    def query_loki_logs_for_kubernetes_cluster(
        self,
        id: str,  # noqa: A002
        *,
        executable="logcli",
        query='{job=~".+"}',
        limit: int = 999,
        output_path="loki_logs.tar.gz",
        extra_args: list[str] | None = None,
        loki_namespace="monitoring-system",
        loki_service="svc/loki-stack",
        loki_port: int = 3100,
    ):
        """
        Queries Loki logs via logcli for the specified cluster.

        Port-forwards to the Loki service on the cluster, then runs logcli
        against the forwarded port. The output is saved as a tar.gz archive.

        Returns the path to the created tar.gz file.
        """
        with self._kubeconfig_for_cluster(id) as kubeconfig:
            with self._port_forward_loki(
                kubeconfig,
                local_port=loki_port,
                namespace=loki_namespace,
                service=loki_service,
            ) as addr:
                proc = self._run_logcli_cmd(
                    executable,
                    "query",
                    f"--addr={addr}",
                    f"--limit={limit}",
                    *(extra_args or []),
                    query,
                )
        assert proc.returncode == 0, "logcli query command failed"
        logger.info(proc.stdout)

        # Write the output to a tar.gz archive
        log_data = proc.stdout
        if isinstance(log_data, str):
            log_data = log_data.encode()
        with tarfile.open(output_path, "w:gz") as tar:
            info = tarfile.TarInfo(name="loki_logs.txt")
            info.size = len(log_data)
            tar.addfile(info, io.BytesIO(log_data))
        logger.info(f"Loki logs archived to {output_path}")
        return output_path

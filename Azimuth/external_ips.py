import typing as t

from robot.api.deco import keyword


class ExternalIpKeywords:
    """
    Keywords for interacting with external IPs.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.external_ips()

    @keyword
    def list_external_ips(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists available external IPs using the active client.
        """
        return list(self._resource.list())

    @keyword
    def fetch_external_ip(self, id: str) -> t.Dict[str, t.Any]:
        """
        Fetches an external IP by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def find_external_ip_by_address(self, ip_address: str) -> t.Dict[str, t.Any]:
        """
        Finds an external IP by IP address using the active client.
        """
        try:
            return next(ip for ip in self._resource.list() if ip.external_ip == ip_address)
        except StopIteration:
            raise ValueError(f"no external IP with address '{ip_address}'")

    @keyword
    def find_free_external_ip(self) -> t.Dict[str, t.Any]:
        """
        Searches for an external IP that is allocated but not assigned and returns it.

        If no such IP exists, an exception is raised.
        """
        try:
            return next(
                ip
                for ip in self._resource.list()
                # Use the available flag if present, falling back to using the machine
                if ip.get("available", ip.machine is None)
            )
        except StopIteration:
            raise ValueError("unable to find an unassigned external IP address")

    @keyword
    def allocate_external_ip(self) -> t.Dict[str, t.Any]:
        """
        Allocates a new external IP and returns it.
        """
        return self._resource.create({})

    @keyword
    def find_free_or_allocate_external_ip(self) -> t.Dict[str, t.Any]:
        """
        Searches for an external IP that is allocated but not assigned and returns it.

        If no such IP exists, a new one is allocated.
        """
        try:
            return self.find_free_external_ip()
        except ValueError:
            return self.allocate_external_ip()

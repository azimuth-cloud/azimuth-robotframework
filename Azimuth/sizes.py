import typing as t

from robot.api.deco import keyword


class SizeKeywords:
    """
    Keywords for interacting with sizes.
    """
    def __init__(self, ctx):
        self._ctx = ctx

    @property
    def _resource(self):
        return self._ctx.client.sizes()

    @keyword
    def list_sizes(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Lists available sizes using the active client.
        """
        return list(self._resource.list())
    
    @keyword
    def fetch_size(self, id: str) -> t.Dict[str, t.Any]:
        """
        Fetches a size by id using the active client.
        """
        return self._resource.fetch(id)

    @keyword
    def find_size_by_name(self, name: str) -> t.Dict[str, t.Any]:
        """
        Finds a size by name using the active client.
        """
        try:
            return next(c for c in self._resource.list() if c.name == name)
        except StopIteration:
            raise ValueError(f"no size with name '{name}'")

    @keyword
    def find_smallest_size_with_resources(
        self,
        *,
        min_cpus: int = 0,
        min_ram: int = 0,
        min_disk: int = 0,
        min_ephemeral_disk: int = 0,
        sort_by: str = "ram,cpus,disk,ephemeral_disk",
        **kwargs
    ) -> t.Dict[str, t.Any]:
        """
        Finds the smallest size that fulfils the specified resource requirements.
        """
        candidates = (
            size
            for size in self._resource.list()
            if (
                size.cpus >= min_cpus and
                size.ram >= min_ram and
                size.disk >= min_disk and
                size.ephemeral_disk >= min_ephemeral_disk
            )
        )
        key_func = lambda size: tuple(getattr(size, attr) for attr in sort_by.split(","))
        try:
            return next(iter(sorted(candidates, key = key_func)))
        except StopIteration:
            raise ValueError("no available sizes fulfilling resource requirements")

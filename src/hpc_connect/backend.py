import abc
import logging
import math
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Generator

from .config import Config
from .schemas import resource_schema

if TYPE_CHECKING:
    from .launch import HPCLauncher
    from .submit import HPCSubmissionManager

logger = logging.getLogger("hpc_connect.backend")


class Backend(abc.ABC):
    name: str

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config.from_defaults()

    @property
    @abc.abstractmethod
    def resource_specs(self) -> list[dict]: ...

    @abc.abstractmethod
    def submission_manager(self) -> "HPCSubmissionManager": ...

    @abc.abstractmethod
    def launcher(self) -> "HPCLauncher": ...

    def supports_subscheduling(self) -> bool:
        return False

    def validate(self) -> None:
        resource_schema.validate({"resources": self.resource_specs})
        nodes = self._resource_index.get("node", [])
        if not nodes:
            raise ValueError("Backend must define node resources")

    @cached_property
    def _resource_index(self) -> dict[str, list[tuple[dict, str | None]]]:
        """Map resource type -> list of (resource_spec, parent_type)"""
        index: dict[str, list[tuple[dict, str | None]]] = {}
        for rspec in self.resource_specs:
            for spec, parent in walk_resources(rspec):
                index.setdefault(spec["type"], []).append((spec, parent))
        return index

    def resource_types(self) -> list[str]:
        """Return the types of resources available"""
        types: set[str] = set()
        for rtype, specs in self._resource_index.items():
            # leaf resources = those with no children
            if all("resources" not in spec or not spec["resources"] for spec, _ in specs):
                types.add(rtype)
        return sorted(types)

    def count_per_node(self, type: str, default: int | None = None) -> int:
        total = 0
        for spec, parent in self._resource_index.get(type, []):
            # Walk up until we hit node
            multiplier = spec["count"]
            p = parent
            while p and p != "node":
                parents = self._resource_index.get(p, [])
                if not parents:
                    break
                multiplier *= parents[0][0]["count"]
                p = parents[0][1]
            if p == "node":
                total += multiplier
        if total:
            return total
        if default is not None:
            return default
        raise ValueError(f"Unable to determine count_per_node for {type!r}") from None

    def count_per_socket(self, type: str, default: int | None = None) -> int:
        for spec, parent in self._resource_index.get(type, []):
            if parent == "socket":
                return spec["count"]
        if default is not None:
            return default
        raise ValueError(f"Unable to determine count_per_socket for {type!r}")

    @cached_property
    def node_count(self) -> int:
        nodes = self._resource_index.get("node", [])
        count = sum(spec["count"] for spec, _ in nodes)
        if count:
            return count
        raise ValueError("Unable to determine node count")

    @cached_property
    def sockets_per_node(self) -> int:
        try:
            count = self.count_per_node("socket")
            return count or 1
        except ValueError:
            return 1

    def nodes_required(self, **types: int) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of as a single MPI
        rank"""
        # backward compatible
        if n := types.pop("max_cpus", None):
            types["cpu"] = n
        if n := types.pop("max_gpus", None):
            types["gpu"] = n
        nodes: int = 1
        for type, count in types.items():
            try:
                per_node = self.count_per_node(type)
            except ValueError:
                continue
            if per_node > 0:
                nodes = max(nodes, int(math.ceil(count / per_node)))
        return nodes

    def resource_view(
        self, *, ranks: int | None = None, ranks_per_socket: int | None = None
    ) -> dict[str, int]:
        """Return basic information about how to allocate resources on this machine for a job
        requiring `ranks` ranks.

        Parameters
        ----------
        ranks : int
            The number of ranks to use for a job
        ranks_per_socket : int
            Number of ranks per socket, for performance use

        Returns
        -------
        view:
          view['np']
          view['ranks']
          view['nodes']
          view['sockets']
          view['ranks_per_socket']

        """
        if ranks is None and ranks_per_socket is not None:
            # Raise an error since there is no reliable way of finding the number of
            # available nodes
            raise ValueError("ranks_per_socket requires ranks also be defined")
        if "socket" not in self._resource_index:
            raise ValueError("resource_view assumes socket-based topology")

        view: dict[str, int] = {
            "np": 0,
            "ranks": 0,
            "ranks_per_socket": 0,
            "nodes": 0,
            "sockets": 0,
        }

        if not ranks and not ranks_per_socket:
            return view

        nodes: int
        if ranks is None and ranks_per_socket is None:
            ranks = ranks_per_socket = 1
            nodes = 1
        elif ranks is not None and ranks_per_socket is None:
            ranks_per_socket = min(ranks, self.count_per_socket("cpu"))
            nodes = int(math.ceil(ranks / self.count_per_socket("cpu") / self.sockets_per_node))
        else:
            assert ranks is not None
            assert ranks_per_socket is not None
            nodes = int(math.ceil(ranks / ranks_per_socket / self.sockets_per_node))
        sockets = int(math.ceil(ranks / ranks_per_socket))  # ty: ignore[unsupported-operator]
        view["np"] = ranks
        view["ranks"] = ranks
        view["ranks_per_socket"] = ranks_per_socket
        view["nodes"] = nodes
        view["sockets"] = sockets
        return view


def walk_resources(
    rspec: dict, *, parent_type: str | None = None
) -> Generator[tuple[dict, str | None], None, None]:
    yield rspec, parent_type
    for child in rspec.get("resources", []) or []:
        yield from walk_resources(child, parent_type=rspec["type"])

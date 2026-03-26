import abc
import copy
import io
import logging
import math
from functools import cached_property
from typing import TYPE_CHECKING
from typing import Any
from typing import Generator

from .schemas import backend_schema
from .schemas import resource_schema

if TYPE_CHECKING:
    from .launch import HPCLauncher
    from .submit import HPCSubmissionManager

logger = logging.getLogger("hpc_connect.backend")

rtype_aliases: dict[str, list[str]] = {
    "cpu": ["CPU", "CPUs", "CPUS", "cpus"],
    "gpu": ["GPU", "GPUs", "GPUS", "gpus"],
    "node": ["NODE", "NODES", "nodes"],
    "socket": ["SOCKET", "SOCKETS", "sockets"],
}


class Backend(abc.ABC):
    name: str

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self._configured: bool = False
        self.config = self.configure(cfg=cfg)
        self._configured = True
        self.aliases: dict[str, str] = {
            alias: canonical for canonical, aliases in rtype_aliases.items() for alias in aliases
        }
        self._resource_index: dict[str, list[tuple[dict, str | None]]] | None = None

    @classmethod
    @abc.abstractmethod
    def default_config(cls) -> dict[str, Any]:
        """Return a complete default configuration for this backend."""
        ...

    @property
    @abc.abstractmethod
    def resource_specs(self) -> list[dict]: ...

    @property
    @abc.abstractmethod
    def valid_launchers(self) -> set[str]: ...

    @classmethod
    def matches(cls, arg: str) -> bool:
        return cls.name == arg

    @abc.abstractmethod
    def submission_manager(self) -> "HPCSubmissionManager": ...

    @abc.abstractmethod
    def launcher(self) -> "HPCLauncher": ...

    def configure(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._configured:
            raise RuntimeError("Backend is frozen; configure() is not allowed")
        cfg = copy.deepcopy(cfg or self.default_config())
        return backend_schema.validate(cfg)

    def describe(self) -> str:
        fp = io.StringIO()
        fp.write(f"Name: {self.name}\n")
        fp.write("Available resources:\n")
        fp.write(f"  Nodes: {self.node_count}\n")
        for rtype in self.resource_types():
            if rtype == "node":
                continue
            fp.write(f"  {rtype}s per node: {self.count_per_node(rtype)}\n")
        return fp.getvalue().strip()

    def supports_subscheduling(self) -> bool:
        return False

    def validate(self) -> None:
        if self.config["launch"]["type"] not in self.valid_launchers:
            type = self.config["launch"]["type"]
            raise ValueError(f"Launcher {type!r} is not supported by {self}")
        for rspec in self.resource_specs:
            self._canonicalize_rspec(rspec)
        resource_schema.validate({"resources": self.resource_specs})
        nodes = self.resource_index.get("node", [])
        if not nodes:
            raise ValueError("Backend must define node resources")

    @property
    def resource_index(self) -> dict[str, list[tuple[dict, str | None]]]:
        if self._resource_index is None:
            self._resource_index = self.make_resource_index()
        assert self._resource_index is not None
        return self._resource_index

    def make_resource_index(self) -> dict[str, list[tuple[dict, str | None]]]:
        """Map resource type -> list of (resource_spec, parent_type)"""
        index: dict[str, list[tuple[dict, str | None]]] = {}
        for rspec in self.resource_specs:
            for spec, parent in walk_resources(rspec):
                spec["type"] = self.canonical_type_name(spec["type"])
                index.setdefault(spec["type"], []).append((spec, parent))
        return index

    def resource_types(self) -> list[str]:
        """Return the types of resources available"""
        types: set[str] = set()
        for rtype, specs in self.resource_index.items():
            # leaf resources = those with no children
            if all("resources" not in spec or not spec["resources"] for spec, _ in specs):
                types.add(rtype)
        return sorted(types)

    def count_per_node(self, rtype: str, default: int | None = None) -> int:
        total = 0
        found = False
        rtype = self.canonical_type_name(rtype)
        for spec, parent in self.resource_index.get(rtype, []):
            # Walk up until we hit node
            multiplier = spec["count"]
            p = parent
            while p and p != "node":
                parents = self.resource_index.get(p, [])
                if not parents:
                    break
                multiplier *= parents[0][0]["count"]
                p = parents[0][1]
            if p == "node":
                found = True
                total += multiplier
        if found:
            return total
        if default is not None:
            return default
        raise ValueError(
            f"Unable to determine count_per_node for {rtype!r} from {self.resource_specs}"
        ) from None

    def count_per_socket(self, rtype: str, default: int | None = None) -> int:
        rtype = self.canonical_type_name(rtype)
        for spec, parent in self.resource_index.get(rtype, []):
            if parent == "socket":
                return spec["count"]
        if default is not None:
            return default
        raise ValueError(f"Unable to determine count_per_socket for {rtype!r}")

    @cached_property
    def node_count(self) -> int:
        nodes = self.resource_index.get("node", [])
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

    def nodes_required(self, **rtypes: int) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of as a single MPI
        rank"""
        # backward compatible
        if n := rtypes.pop("max_cpus", None):
            rtypes["cpu"] = n
        if n := rtypes.pop("max_gpus", None):
            rtypes["gpu"] = n
        rtypes = {self.canonical_type_name(k): v for k, v in rtypes.items()}
        nodes: int = 1
        for rtype, count in rtypes.items():
            try:
                per_node = self.count_per_node(rtype)
            except ValueError:
                continue
            if per_node > 0:
                nodes = max(nodes, int(math.ceil(count / per_node)))
        return nodes

    def _canonicalize_rspec(self, rspec: dict) -> None:
        rspec["type"] = self.canonical_type_name(rspec["type"])
        for child in rspec.get("resources", []) or []:
            self._canonicalize_rspec(child)

    def canonical_type_name(self, rtype: str) -> str:
        if canonical := self.aliases.get(rtype):
            return canonical
        return rtype

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
        if "socket" not in self.resource_index:
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

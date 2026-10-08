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
from .topology import HeterogeneousTopologyError
from .topology import Topology

if TYPE_CHECKING:
    from .launch import HPCLauncher
    from .launch import LaunchAdapter
    from .submit import SubmissionManagerProtocol

logger = logging.getLogger("hpc_connect.backend")

rtype_aliases: dict[str, list[str]] = {
    "cpu": ["CPU", "CPUs", "CPUS", "cpus"],
    "gpu": ["GPU", "GPUs", "GPUS", "gpus"],
    "node": ["NODE", "NODES", "nodes"],
    "socket": ["SOCKET", "SOCKETS", "sockets"],
}


class Backend(abc.ABC):
    type: str

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self._configured: bool = False
        self.config = self.configure(cfg=cfg)
        self._configured = True
        self.aliases: dict[str, str] = {
            alias: canonical for canonical, aliases in rtype_aliases.items() for alias in aliases
        }
        self._resource_index: dict[str, list[tuple[dict, str | None]]] | None = None
        self._topology: Topology | None = None

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
        return cls.type == arg

    @property
    def name(self) -> str:
        return self.config.get("name") or self.type

    @abc.abstractmethod
    def submission_manager(self) -> "SubmissionManagerProtocol": ...

    @abc.abstractmethod
    def launcher(self) -> "HPCLauncher": ...

    @abc.abstractmethod
    def launch_adapter(self) -> "LaunchAdapter": ...

    def configure(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._configured:
            raise RuntimeError("Backend is frozen; configure() is not allowed")
        cfg = copy.deepcopy(cfg or self.default_config())
        return backend_schema.validate(cfg)

    def build_launch_argv(self, args: list[str]) -> list[str]:
        return self.launch_adapter().build_argv(args)

    def launch(self, args: list[str], **kwargs: Any):
        return self.launcher().submit(args, **kwargs)

    def describe(self) -> str:
        fp = io.StringIO()
        fp.write(f"Name: {self.name}\n")
        fp.write(f"Type: {self.type}\n")
        fp.write("Available resources:\n")
        fp.write(f"  Nodes: {self.node_count}\n")
        for rtype in self.resource_types():
            if rtype == "node":
                continue
            fp.write(f"  {rtype}s per node: {self.count_per_node(rtype)}\n")
        return fp.getvalue().strip()

    def supports_subscheduling(self) -> bool:
        return False

    def supports_dependencies(self) -> bool:
        return False

    def validate(self) -> None:
        if self.config["launch"]["type"] not in self.valid_launchers:
            type = self.config["launch"]["type"]
            raise ValueError(f"Launcher {type!r} is not supported by {self}")
        for rspec in self.resource_specs:
            self._canonicalize_rspec(rspec)
        resource_schema.validate({"resources": self.resource_specs})
        nodes = self.topology.by_type("node")
        if not nodes:
            raise ValueError("Backend must define node resources")

    @property
    def topology(self) -> Topology:
        if self._topology is None:
            self._topology = self.make_topology()
        return self._topology

    def make_topology(self) -> Topology:
        return Topology.from_resource_specs(self.resource_specs)

    def is_homogeneous(self) -> bool:
        return self.topology.is_homogeneous()

    def total_resources(self, rtype: str) -> int:
        return self.topology.total(self.canonical_type_name(rtype))

    def max_per_node(self, rtype: str) -> int:
        return self.topology.max_per_node(self.canonical_type_name(rtype))

    def min_per_node(self, rtype: str) -> int:
        return self.topology.min_per_node(self.canonical_type_name(rtype))

    def uniform_per_node(self, rtype: str) -> int:
        return self.topology.uniform_per_node(self.canonical_type_name(rtype))

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
        types = {entry.type for entry in self.topology.entries if not entry.children}
        return sorted(types)

    def count_per_node(self, rtype: str, default: int | None = None) -> int:
        rtype = self.canonical_type_name(rtype)
        try:
            return self.topology.uniform_per_node(rtype)
        except HeterogeneousTopologyError:
            raise
        except ValueError:
            if default is not None:
                return default
            raise ValueError(
                f"Unable to determine count_per_node for {rtype!r} from {self.resource_specs}"
            ) from None

    def count_per_socket(self, rtype: str, default: int | None = None) -> int:
        rtype = self.canonical_type_name(rtype)
        try:
            return self.topology.uniform_per_socket(rtype)
        except HeterogeneousTopologyError:
            raise
        except ValueError:
            if default is not None:
                return default
            raise ValueError(f"Unable to determine count_per_socket for {rtype!r}")

    @cached_property
    def node_count(self) -> int:
        nodes = self.topology.by_type("node")
        count = sum(entry.count for entry in nodes)
        if count:
            return count
        raise ValueError("Unable to determine node count")

    @cached_property
    def sockets_per_node(self) -> int:
        try:
            count = self.uniform_per_node("socket")
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
        if not self.is_homogeneous():
            raise HeterogeneousTopologyError(
                "nodes_required() requires a homogeneous topology with uniform per-node resource counts; "
                "for heterogeneous systems, inspect node_groups() and plan the allocation explicitly"
            )
        nodes: int = 1
        for rtype, count in rtypes.items():
            per_node = self.count_per_node(rtype, default=0)
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
        if not self.topology.by_type("socket"):
            raise ValueError("resource_view assumes socket-based topology")
        if not self.is_homogeneous():
            raise HeterogeneousTopologyError(
                "resource_view() requires a homogeneous topology with uniform socket counts; "
                "for heterogeneous systems, inspect node_groups() and compute the launch layout explicitly"
            )

        view: dict[str, int] = {"np": 0, "ranks": 0, "ranks_per_socket": 0, "nodes": 0, "sockets": 0}

        if not ranks and not ranks_per_socket:
            return view

        nodes: int
        if ranks is None and ranks_per_socket is None:
            ranks = ranks_per_socket = 1
            nodes = 1
        elif ranks is not None and ranks_per_socket is None:
            cpus_per_socket = self.count_per_socket("cpu")
            ranks_per_socket = min(ranks, cpus_per_socket)
            nodes = int(math.ceil(ranks / cpus_per_socket / self.sockets_per_node))
        else:
            assert ranks is not None
            assert ranks_per_socket is not None
            nodes = int(math.ceil(ranks / ranks_per_socket / self.sockets_per_node))
        sockets = int(math.ceil(ranks / ranks_per_socket))
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

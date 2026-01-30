# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import logging
import math
import os
import sys
from functools import cached_property
from typing import IO
from typing import Any
from typing import Literal
from typing import cast

import yaml

from .discover import default_resource_set
from .pluginmanager import HPCConnectPluginManager
from .schemas import config_schema
from .schemas import environment_variable_schema
from .util import collections
from .util import safe_loads

default_config_values: dict[str, Any] = {
    "debug": False,
    "plugins": [],
    "machine": {"resources": None},
    "submit": {"backend": None, "default_options": []},
    "launch": {
        "exec": "mpiexec",
        "numproc_flag": "-n",
        "default_options": [],
        "local_options": [],
        "pre_options": [],
        "mappings": {},
    },
}

ConfigScopes = Literal["site", "global", "local"]


class Config:
    def __init__(self) -> None:
        self.pluginmanager: HPCConnectPluginManager = HPCConnectPluginManager()
        self.data: dict[str, Any] = dict(default_config_values)
        for name in ("site", "global", "local"):
            scope = get_config_scope_data(cast(ConfigScopes, name))
            self.data = collections.merge(self.data, scope)  # type: ignore
        if env_scope := get_env_scope():
            self.data = collections.merge(self.data, env_scope)  # type: ignore
        if self.data["debug"]:
            logging.getLogger("hpc_connect").setLevel(logging.DEBUG)

    def get(self, path: str, default: Any = None) -> Any:
        parts = process_config_path(path)
        if parts[0] == "config":
            # Legacy support for top level config key
            parts = parts[1:]
        value = self.data.get(parts[0], {})
        for key in parts[1:]:
            # cannot use value.get(key, default) in case there is another part
            # and default is not a dict
            if key not in value:
                return default
            value = value[key]
        return value

    def set(self, path: str, value: Any) -> None:
        parts = process_config_path(path)
        data = value
        for key in reversed(parts):
            data = {key: data}
        data = config_schema.validate(data)
        self.data = collections.merge(self.data, data)  # type: ignore

    def set_main_options(self, args: argparse.Namespace) -> None:
        """Set main configuration options based on command-line arguments.

        Updates the configuration attributes based on the provided argparse Namespace containing
        command-line arguments.

        Args:
            args: An argparse.Namespace object containing command-line arguments.
        """
        if args.config_mods:
            data: dict[str, Any] = {}
            for fullpath in args.config_mods:
                current = data
                components = process_config_path(fullpath)
                for component in components[:-2]:
                    current = current.setdefault(component, {})
                current[components[-2]] = safe_loads(components[-1])
            self.data = collections.merge(self.data, data)  # type: ignore
            if self.data["debug"]:
                logging.getLogger("hpc_connect").setLevel(logging.DEBUG)

    @property
    def resource_specs(self) -> list[dict]:
        if self.data["machine"]["resources"] is None:
            self.data["machine"]["resources"] = default_resource_set()
        return self.data["machine"]["resources"]

    def resource_types(self) -> list[str]:
        """Return the types of resources available"""
        types: set[str] = set()
        for rspec in self.resource_specs:
            if rspec["type"] == "node":
                for spec in rspec["resources"]:
                    if spec["type"] == "socket":
                        types.update([child["type"] for child in spec["resources"]])
        return sorted(types)

    def count_per_rspec(self, rspec: dict[str, Any], type: str) -> int | None:
        for child in rspec["resources"]:
            if child["type"] == type:
                return child["count"]
            elif type.endswith("s") and child["type"] == type[:-1]:
                return child["count"]
        return None

    def count_per_node(self, type: str, default: int | None = None) -> int:
        for rspec in self.resource_specs:
            if rspec["type"] == "node":
                count = self.count_per_rspec(rspec, type)
                if count is not None:
                    return count
        try:
            count_per_socket = self.count_per_socket(type)
        except ValueError:
            if default is not None:
                return default
            raise ValueError(f"Unable to determine count_per_node for {type!r}") from None
        else:
            return count_per_socket * self.sockets_per_node

    def count_per_socket(self, type: str, default: int | None = None) -> int:
        for rspec1 in self.resource_specs:
            if rspec1["type"] == "node":
                for rspec2 in rspec1["resources"]:
                    if rspec2["type"] == "socket":
                        count = self.count_per_rspec(rspec2, type)
                        if count is not None:
                            return count
        if default is not None:
            return default
        raise ValueError(f"Unable to determine count_per_socket for {type!r}")

    @cached_property
    def node_count(self) -> int:
        count: int = 0
        for resource in self.resource_specs:
            if resource["type"] == "node":
                count += resource["count"]
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
                count_per_node = self.count_per_node(type)
            except ValueError:
                continue
            else:
                if count_per_node == 0:
                    continue
            nodes = max(nodes, int(math.ceil(count / count_per_node)))
        return nodes

    def compute_required_resources(
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
        SimpleNamespace

        """
        if ranks is None and ranks_per_socket is not None:
            # Raise an error since there is no reliable way of finding the number of
            # available nodes
            raise ValueError("ranks_per_socket requires ranks also be defined")

        reqd_resources: dict[str, int] = {
            "np": 0,
            "ranks": 0,
            "ranks_per_socket": 0,
            "nodes": 0,
            "sockets": 0,
        }

        if not ranks and not ranks_per_socket:
            return reqd_resources

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
        reqd_resources["np"] = ranks
        reqd_resources["ranks"] = ranks
        reqd_resources["ranks_per_socket"] = ranks_per_socket
        reqd_resources["nodes"] = nodes
        reqd_resources["sockets"] = sockets
        return reqd_resources

    def dump(self, stream: IO[Any], scope: str | None = None, **kwargs: Any) -> None:
        data: dict[str, Any] = {}
        for section in self.scopes["defaults"]:
            if section == "machine":
                continue
            section_data = self.get_config(section, scope=scope)
            if not section_data and scope is not None:
                continue
            data[section] = section_data
        data.setdefault("machine", {})["resources"] = self.resource_specs
        yaml.dump({"hpc_connect": data}, stream, **kwargs)


def get_config_scope_data(scope: ConfigScopes) -> dict[str, Any]:
    """Read the data from config scope ``data``

    By the time the data leaves, it is validated and does not contain a top-level ``canary`` field

    """
    data: dict[str, Any] = {}
    file = get_scope_filename(scope)
    if file is not None and (fd := read_config_file(file)):
        data.update(fd)
    return config_schema.validate(data)


def get_scope_filename(scope: str) -> str | None:
    if scope == "site":
        if var := os.getenv("HPC_CONNECT_SITE_CONFIG"):
            return var
        return os.path.join(sys.prefix, "etc/hpc_connect/config.yaml")
    elif scope == "global":
        if var := os.getenv("HPC_CONNECT_GLOBAL_CONFIG"):
            return var
        elif var := os.getenv("XDG_CONFIG_HOME"):
            file = os.path.join(var, "hpc_connect/config.yaml")
            if os.path.exists(file):
                return file
        return os.path.expanduser("~/.config/hpc_connect.yaml")
    elif scope == "local":
        return os.path.abspath("./hpc_connect.yaml")
    raise ValueError(f"Could not determine filename for scope {scope!r}")


def get_env_scope() -> dict[str, Any]:
    variables = {key: var for key, var in os.environ.items() if key.startswith("HPC_CONNECT_")}
    if variables:
        variables = environment_variable_schema.validate(variables)
    return variables


def read_config_file(file: str) -> dict[str, Any] | None:
    """Load configuration settings from ``file``"""
    if not os.path.exists(file):
        return None
    with open(file) as fh:
        return yaml.safe_load(fh)


def process_config_path(path: str) -> list[str]:
    result: list[str] = []
    if path.startswith(":"):
        raise ValueError(f"Illegal leading ':' in path {path}")
    while path:
        front, _, path = path.partition(":")
        result.append(front)
        if path.startswith(("{", "[")):
            result.append(path)
            return result
    return result


class LocalScopeDoesNotExistError(Exception):
    pass

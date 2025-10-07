# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import logging
import math
import os
import sys
from collections.abc import ValuesView
from functools import cached_property
from typing import IO
from typing import Any

import schema
import yaml

from .discover import default_resource_set
from .pluginmanager import HPCConnectPluginManager
from .schemas import config_schema
from .schemas import environment_variable_schema
from .schemas import launch_schema
from .schemas import machine_schema
from .schemas import submit_schema
from .util import collections
from .util import safe_loads
from .util.string import strip_quotes

logger = logging.getLogger("hpc_connect")

section_schemas: dict[str, schema.Schema] = {
    "config": config_schema,
    "machine": machine_schema,
    "submit": submit_schema,
    "launch": launch_schema,
}


class ConfigScope:
    def __init__(self, name: str, file: str | None, data: dict[str, Any]) -> None:
        self.name = name
        self.file = file
        self.data: dict[str, Any] = {}
        for section, data in data.items():
            schema = section_schemas[section]
            self.data[section] = schema.validate(data)

    def __repr__(self):
        file = self.file or "<none>"
        return f"ConfigScope({self.name}: {file})"

    def __eq__(self, other):
        if not isinstance(other, ConfigScope):
            return False
        return self.name == other.name and self.file == other.file and other.data == self.data

    def __iter__(self):
        return iter(self.data)

    def __contains__(self, section: str) -> bool:
        return section in self.data

    def get_section(self, section: str) -> Any:
        return self.data.get(section)

    def pop_section(self, section: str) -> Any:
        return self.data.pop(section, None)

    def dump(self) -> None:
        if self.file is None:
            return
        with open(self.file, "w") as fh:
            yaml.dump({"hpc_connect": self.data}, fh, default_flow_style=False)


class Config:
    def __init__(self) -> None:
        self.pluginmanager: HPCConnectPluginManager = HPCConnectPluginManager()
        rspec = self.pluginmanager.hook.hpc_connect_discover_resources()
        defaults = {
            "config": {
                "debug": False,
                "plugins": [],
            },
            "machine": {
                "resources": rspec,
            },
            "submit": {
                "backend": None,
                "default_options": [],
            },
            "launch": {
                "exec": "mpiexec",
                "numproc_flag": "-n",
                "default_options": [],
                "local_options": [],
                "pre_options": [],
                "mappings": {},
            },
        }
        self.scopes: dict[str, ConfigScope] = {}
        default_scope = ConfigScope("defaults", None, defaults)
        self.push_scope(default_scope)
        for scope in ("site", "global", "local"):
            config_scope = read_config_scope(scope)
            self.push_scope(config_scope)
        if cscope := read_env_config():
            self.push_scope(cscope)
        if self.get("config:debug"):
            set_logging_level("debug")

    def read_only_scope(self, scope: str) -> bool:
        return scope in ("defaults", "environment", "command_line")

    def push_scope(self, scope: ConfigScope) -> None:
        self.scopes[scope.name] = scope
        if cfg := scope.get_section("config"):
            if plugins := cfg.get("plugins"):
                for f in plugins:
                    self.pluginmanager.consider_plugin(f)

    def pop_scope(self, scope: ConfigScope) -> ConfigScope | None:
        return self.scopes.pop(scope.name, None)

    def get_config(self, section: str, scope: str | None = None) -> Any:
        scopes: ValuesView[ConfigScope] | list[ConfigScope]
        if scope is None:
            scopes = self.scopes.values()
        else:
            scopes = [self.validate_scope(scope)]
        merged_section: dict[str, Any] = {}
        for config_scope in scopes:
            data = config_scope.get_section(section)
            if not data or not isinstance(data, dict):
                continue
            merged_section = collections.merge(merged_section, {section: data})
        if section not in merged_section:
            return {}
        return merged_section[section]

    def get(self, path: str, default: Any = None, scope: str | None = None) -> Any:
        parts = process_config_path(path)
        section = parts.pop(0)
        value = self.get_config(section, scope=scope)
        while parts:
            key = parts.pop(0)
            # cannot use value.get(key, default) in case there is another part
            # and default is not a dict
            if key not in value:
                return default
            value = value[key]
        return value

    def get_highest_priority(self, path: str, default: Any = None) -> tuple[Any, str]:
        sentinel = object()
        for scope in reversed(self.scopes.keys()):
            value = self.get(path, default=sentinel, scope=scope)
            if value is not sentinel:
                return value, scope
        return default, "none"

    def set(self, path: str, value: Any, scope: str | None = None) -> None:
        parts = process_config_path(path)
        section = parts.pop(0)
        section_data = self.get_config(section, scope=scope)
        data = section_data
        while len(parts) > 1:
            key = parts.pop(0)
            new = data.get(key, {})
            if isinstance(new, dict):
                new = dict(new)
                # reattach to parent object
                data[key] = new
            data = new
        # update new value
        data[parts[0]] = value
        self.update_config(section, section_data, scope=scope)

    def add(self, fullpath: str, scope: str | None = None) -> None:
        path: str = ""
        existing: Any = None
        components = process_config_path(fullpath)
        has_existing_value = True
        for idx, name in enumerate(components[:-1]):
            path = name if not path else f"{path}:{name}"
            existing = self.get(path, scope=scope)
            if existing is None:
                has_existing_value = False
                # construct value from this point down
                value = safe_loads(components[-1])
                for component in reversed(components[idx + 1 : -1]):
                    value = {component: value}
                break

        if has_existing_value:
            path = ":".join(components[:-1])
            value = safe_loads(strip_quotes(components[-1]))
            existing = self.get(path, scope=scope)

        if isinstance(existing, list) and not isinstance(value, list):
            # append values to lists
            value = [value]

        new = collections.merge(existing, value)
        self.set(path, new, scope=scope)

    def highest_precedence_scope(self) -> ConfigScope:
        """Non-internal scope with highest precedence."""
        file_scopes = [scope for scope in self.scopes.values() if scope.file is not None]
        return next(reversed(file_scopes))

    def validate_scope(self, scope: str | None) -> ConfigScope:
        if scope is None:
            return self.highest_precedence_scope()
        elif scope in self.scopes:
            return self.scopes[scope]
        elif scope == "internal":
            self.scopes["internal"] = ConfigScope("internal", None, {})
            return self.scopes["internal"]
        else:
            raise ValueError(f"Invalid scope {scope!r}")

    def update_config(self, section: str, update_data: dict[str, Any], scope: str | None = None):
        """Update the configuration file for a particular scope.

        Args:
            section (str): section of the configuration to be updated
            update_data (dict): data to be used for the update
            scope (str): scope to be updated
        """
        if scope is None:
            config_scope = self.highest_precedence_scope()
        else:
            config_scope = self.scopes[scope]
        # read only the requested section's data.
        config_scope.data[section] = dict(update_data)
        config_scope.dump()

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
            scope = ConfigScope("command_line", None, data)
            self.push_scope(scope)
            if self.get("config:debug", scope="command_line"):
                set_logging_level("debug")

    @property
    def resource_specs(self) -> list[dict]:
        specs, _ = self.get_highest_priority("machine:resources")
        if specs is not None:
            return specs
        resources = default_resource_set()
        self.set("machine:resources", specs, scope="defaults")
        return resources

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


def read_config_scope(scope: str) -> ConfigScope:
    data: dict[str, Any] = {}
    if file := get_scope_filename(scope):
        if fd := read_config_file(file):
            if "hpc_connect" not in fd:
                raise KeyError("Missing key 'hpc_connect'")
            data.update(fd["hpc_connect"])
    return ConfigScope(scope, file, data)


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


def read_env_config() -> ConfigScope | None:
    variables = {key: var for key, var in os.environ.items() if key.startswith("HPC_CONNECT_")}
    if not variables:
        return None
    data = environment_variable_schema.validate(variables)
    return ConfigScope("environment", None, data)


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


ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("==> %(message)s"))
logger.addHandler(ch)


loglevelmap: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def set_debug(arg: bool) -> None:
    if arg:
        set_logging_level("DEBUG")


def set_logging_level(levelname: str) -> None:
    level = loglevelmap[levelname.upper()]
    for h in logger.handlers:
        h.setLevel(level)
    logger.setLevel(level)

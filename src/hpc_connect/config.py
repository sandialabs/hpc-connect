# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import copy
import logging
import os
import sys
from typing import Any
from typing import Literal
from typing import cast

import yaml

from .schemas import config_schema
from .util import collections
from .util import safe_loads
from .util.serialize import deserialize
from .util.serialize import serialize

ConfigScopes = Literal["site", "global", "local"]


class Config:
    def __init__(self, export: bool = False) -> None:
        self.data: dict[str, Any]
        if var := os.getenv("HPC_CONNECT_CFG64"):
            self.data = self.validate(deserialize(var))
        else:
            data: dict[str, Any] = {}
            for name in ("site", "global", "local"):
                scope = get_config_scope_data(cast(ConfigScopes, name))
                collections.merge(data, scope)  # type: ignore
            self.data = self.validate(data)
        if export:
            self.export()

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def validate(self, data: dict[str, Any]) -> dict[str, Any]:
        validated = config_schema.validate(data)
        return validated

    def set_main_options(self, args: argparse.Namespace) -> None:
        """Set main configuration options based on command-line arguments.

        Updates the configuration attributes based on the provided argparse Namespace containing
        command-line arguments.

        Args:
            args: An argparse.Namespace object containing command-line arguments.
        """
        if args.config_mods:
            overlay: dict[str, Any] = {}
            for fullpath in args.config_mods:
                current = overlay
                components = process_config_path(fullpath)
                for component in components[:-2]:
                    current = current.setdefault(component, {})
                current[components[-2]] = safe_loads(components[-1])
            candidate = copy.deepcopy(self.data)
            collections.merge(candidate, overlay)  # type: ignore
            self.data = self.validate(candidate)
            if self.data.get("debug"):
                logging.getLogger("hpc_connect").setLevel(logging.DEBUG)
            self.export()

    def set(self, path: str, value: Any) -> None:
        """Set the configuration value using yaml-like syntax for path.

        Examples:

        >>> config = Config()
        >>> config.set("backend:default_options", ["--account=ABC123"])

        """
        # the thing
        overlay: dict[str, Any] = {}
        current = overlay
        components = path.split(":")
        for component in components[:-1]:
            current = current.setdefault(component, {})
        current[components[-1]] = value

        candidate = copy.deepcopy(self.data)
        collections.merge(candidate, overlay)  # type: ignore
        self.data = self.validate(candidate)
        self.export()

    def backend(self, name: str) -> dict[str, Any] | None:
        for entry in self.data.get("backends") or []:
            if entry.get("name") == name:
                return entry
        return None

    def export(self) -> str:
        s = serialize(self.data)
        os.environ["HPC_CONNECT_CFG64"] = s
        return s


def get_config_scope_data(scope: ConfigScopes) -> dict[str, Any]:
    """Read the data from config scope ``data``

    By the time the data leaves, it is validated and does not contain a top-level ``canary`` field

    """
    data: dict[str, Any] = {}
    file = get_scope_filename(scope)
    if file is not None and (fd := read_config_file(file)):
        data.update(fd)
    return data


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


def read_config_file(file: str) -> dict[str, Any] | None:
    """Load configuration settings from ``file``"""
    if not os.path.exists(file):
        return None
    with open(file) as fh:
        fd = yaml.safe_load(fh)
        if not isinstance(fd, dict):
            raise TypeError(f"{file}: expected mapping at top level")
        if "hpc_connect" in fd:
            fd = fd["hpc_connect"]
        return fd


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


_config: Config | None = None


def get_config(export: bool = False) -> Config:
    global _config
    if _config is None:
        _config = Config(export=export)
    elif export:
        _config.export()
    assert _config is not None
    return _config


def export() -> str:
    global _config
    if _config is None:
        _config = Config()
    return _config.export()


def reset() -> None:
    global _config
    _config = None
    os.environ.pop("HPC_CONNECT_CFG64", None)


def __getattr__(name: str) -> Any:
    return getattr(get_config(), name)

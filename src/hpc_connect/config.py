# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import dataclasses
import logging
import os
import shutil
import sys
from typing import Any
from typing import Literal
from typing import cast

import yaml

from .schemas import config_schema
from .schemas import environment_variable_schema
from .util import collections
from .util import safe_loads

ConfigScopes = Literal["site", "global", "local"]


@dataclasses.dataclass
class MPMDConfig:
    local_options: list[str] = dataclasses.field(default_factory=list)
    global_options: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class LaunchConfig:
    exec: str
    numproc_flag: str
    pre_options: list[str] = dataclasses.field(default_factory=list)
    default_options: list[str] = dataclasses.field(default_factory=list)
    mappings: dict[str, str] = dataclasses.field(default_factory=dict)
    mpmd: MPMDConfig = dataclasses.field(default_factory=MPMDConfig)


@dataclasses.dataclass
class RawLaunchConfig:
    exec: str = ""
    numproc_flag: str = ""
    pre_options: list[str] = dataclasses.field(default_factory=list)
    default_options: list[str] = dataclasses.field(default_factory=list)
    mappings: dict[str, str] = dataclasses.field(default_factory=dict)
    overrides: dict[str, "LaunchConfig"] = dataclasses.field(default_factory=dict)
    mpmd: MPMDConfig = dataclasses.field(default_factory=MPMDConfig)

    def __post_init__(self) -> None:
        if not self.exec:
            self.exec = shutil.which("mpiexec") or "mpiexec"
        if not self.numproc_flag:
            self.numproc_flag = "-n"

    def resolve(self, name: str) -> LaunchConfig:
        if name not in self.overrides:
            return LaunchConfig(
                exec=self.exec,
                numproc_flag=self.numproc_flag,
                pre_options=self.pre_options,
                default_options=self.default_options,
                mappings=self.mappings,
                mpmd=self.mpmd,
            )
        override = self.overrides[name]
        return LaunchConfig(
            exec=override.exec or self.exec,
            numproc_flag=override.numproc_flag or self.numproc_flag,
            pre_options=override.pre_options or self.pre_options,
            default_options=override.default_options or self.default_options,
            mappings=override.mappings or self.mappings,
            mpmd=override.mpmd or self.mpmd,
        )


@dataclasses.dataclass
class SubmitConfig:
    default_options: list[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class RawSubmitConfig:
    default_options: list[str] = dataclasses.field(default_factory=list)
    overrides: dict[str, "SubmitConfig"] = dataclasses.field(default_factory=dict)

    def resolve(self, name: str) -> SubmitConfig:
        if name not in self.overrides:
            return SubmitConfig(default_options=self.default_options)
        override = self.overrides[name]
        return SubmitConfig(default_options=override.default_options or self.default_options)


@dataclasses.dataclass
class Config:
    debug: bool = False
    backend: str = ""
    submit: RawSubmitConfig = dataclasses.field(default_factory=RawSubmitConfig)
    launch: RawLaunchConfig = dataclasses.field(default_factory=RawLaunchConfig)

    def __post_init__(self) -> None:
        if not self.backend:
            self.backend = "local"
        if self.debug:
            logging.getLogger("hpc_connect").setLevel(logging.DEBUG)

    @classmethod
    def from_defaults(
        cls,
        files: bool = True,
        env: bool = True,
        overrides: dict[str, Any] | None = None,
    ) -> "Config":
        data: dict[str, Any] = {}
        if files:
            for name in ("site", "global", "local"):
                scope = get_config_scope_data(cast(ConfigScopes, name))
                data = collections.merge(data, scope)  # type: ignore
        if env:
            if env_scope := get_env_scope():
                data = collections.merge(data, env_scope)  # type: ignore
        if overrides:
            config_schema.validate(overrides)
            data = collections.merge(data, overrides)  # type: ignore
        kwds: dict[str, Any] = {}
        if fd := data.pop("launch", None):
            fields = {f.name for f in dataclasses.fields(RawLaunchConfig)}
            for key in list(fd.keys()):
                if key not in fields:
                    o = fd.pop(key)
                    fd.setdefault("overrides", {}).update({key: RawLaunchConfig(**o)})
            kwds["launch"] = RawLaunchConfig(**fd)
        if fd := data.pop("submit", None):
            fields = {f.name for f in dataclasses.fields(RawSubmitConfig)}
            for key in list(fd.keys()):
                if key not in fields:
                    o = fd.pop(key)
                    fd.setdefault("overrides", {}).update({key: RawSubmitConfig(**o)})
            kwds["submit"] = RawSubmitConfig(**fd)
        if fd := data.pop("mpmd", None):
            kwds["mpmd"] = MPMDConfig(**fd)
        kwds.update(data)
        self = cls(**kwds)
        return self

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
            data = collections.merge(self.data, data)  # type: ignore
            if data.get("debug"):
                logging.getLogger("hpc_connect").setLevel(logging.DEBUG)
            if fd := data.pop("launch", None):
                for key, value in fd.items():
                    setattr(self.launch, key, value)
            if fd := data.pop("submit", None):
                for key, value in fd.items():
                    setattr(self.submit, key, value)
            for key, value in data.items():
                setattr(self, key, value)


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
        fd = yaml.safe_load(fh)
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

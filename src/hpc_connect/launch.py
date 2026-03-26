# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import copy
import logging
import os
import shlex
import shutil
import subprocess
from typing import Any
from typing import Sequence

from .backend import Backend
from .schemas import launch_schema

logger = logging.getLogger("hpc_connect.launch")


class HPCLauncher:
    def __init__(self, adapter: "LaunchAdapter") -> None:
        self.adapter = adapter

    def __call__(
        self, args: list[str], echo: bool = False, **kwargs: Any
    ) -> subprocess.CompletedProcess:
        return self.submit(args, echo=echo, **kwargs)

    def submit(
        self, args: list[str], echo: bool = False, **kwargs: Any
    ) -> subprocess.CompletedProcess:
        argv = self.adapter.build_argv(args)
        if echo:
            print(f"Command line: {shlex.join(argv)}")
        env = kwargs.get("env") or os.environ.copy()
        if variables := self.adapter.config.get("variables"):
            env.update(variables)
        return subprocess.run(argv, env=env, **kwargs)


class LaunchAdapter:
    name: str

    def __init__(self, *, config: dict[str, Any], backend: "Backend") -> None:
        self.config = launch_schema.validate(copy.deepcopy(config))
        self.backend = backend

    def build_argv(self, args: list[str]) -> list[str]:
        specs = self.parse(args)
        return self.join_specs(specs)

    def join_specs(self, specs: list["LaunchSpec"]) -> list[str]:
        raise NotImplementedError

    def parse(self, args: list[str]) -> list["LaunchSpec"]:
        parser = ArgumentParser(numproc_flag=self.config["numproc_flag"])
        return parser.parse_args(args)

    @staticmethod
    def expand_inplace(args: list[str], **kwargs: Any) -> None:
        for i, arg in enumerate(args):
            args[i] = LaunchAdapter.expand_one(arg, **kwargs)

    @staticmethod
    def expand_one(arg: str, **kwargs: Any) -> str:
        try:
            return str(arg) % kwargs
        except Exception:
            return arg


class LaunchSpec:
    def __init__(self, args: list[str], processes: int | None = None) -> None:
        self.args = list(args)
        self.processes = processes

    def __repr__(self) -> str:
        return f"LaunchSpec({shlex.join(self.args)})"

    def partition(self) -> tuple[list[str], list[str]]:
        i = argp(self.args)
        if i == -1:
            return [], list(self.args)
        else:
            return self.args[:i], self.args[i:]


class ArgumentParser:
    def __init__(self, *, numproc_flag: str | None = None) -> None:
        self.numproc_flag: str = numproc_flag or "-n"

    def parse_args(self, args: Sequence[str]) -> list[LaunchSpec]:
        """Inspect arguments to launch to infer number of processors requested"""
        numproc_flags = {"-n", "-np"}
        numproc_flags.add(self.numproc_flag)
        launchspecs: list[LaunchSpec] = []
        spec: list[str] = []
        processes: int | None = None
        command_seen: bool = False
        iter_args = iter(args or [])
        while True:
            try:
                arg = next(iter_args)
            except StopIteration:
                break
            if shutil.which(arg):
                command_seen = True
            if not command_seen:
                if arg in numproc_flags:
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif any(arg.startswith(f"{f}=") for f in numproc_flags):
                    i = len(arg.partition("=")[0]) + 1
                    processes = int(arg[i:])
                    spec.append(arg)
                else:
                    spec.append(arg)
            elif arg == ":":
                # MPMD: end of this segment
                launchspecs.append(LaunchSpec(spec, processes))
                spec = []
                command_seen, processes = False, None
            else:
                spec.append(arg)

        if spec:
            launchspecs.append(LaunchSpec(spec, processes))

        return launchspecs


def argp(args: list[str]) -> int:
    for i, arg in enumerate(args):
        if shutil.which(arg):
            return i
    return -1


def launch(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    from . import get_backend

    backend = get_backend()
    launcher = backend.launcher()
    return launcher.submit(args, **kwargs)

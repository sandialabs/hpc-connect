# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import logging
import shlex
import shutil
import subprocess
from typing import Any
from typing import Sequence

from .backend import Backend
from .config import LaunchConfig

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
        return subprocess.run(argv, **kwargs)


class LaunchAdapter:
    name: str

    def __init__(self, *, config: "LaunchConfig", backend: "Backend") -> None:
        self.config = config
        self.backend = backend

    def build_argv(self, args: list[str]) -> list[str]:
        specs = self.parse(args)
        return self.join_specs(specs)

    def join_specs(self, specs: list["LaunchSpec"]) -> list[str]:
        raise NotImplementedError

    def parse(self, args: list[str]) -> list["LaunchSpec"]:
        parser = ArgumentParser(
            mappings=self.config.mappings, numproc_flag=self.config.numproc_flag or "-n"
        )
        return parser.parse_args(args)


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
    def __init__(self, *, mappings: dict[str, str] | None, numproc_flag: str | None = None) -> None:
        self.mappings: dict[str, str] = mappings or {}
        self.numproc_flag: str = numproc_flag or "-n"
        if "-n" not in self.mappings:
            # always map -n to numproc_flag
            self.mappings["-n"] = self.numproc_flag

    def mapped(self, arg: str) -> str | None:
        if arg in self.mappings:
            return self.mappings[arg]
        # check for the case of long opt: arg=
        for pat, repl in self.mappings.items():
            if arg.startswith(f"{pat}="):
                return arg.replace(pat, repl)
        return None

    def parse_args(self, args: Sequence[str]) -> list[LaunchSpec]:
        """Inspect arguments to launch to infer number of processors requested"""
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
                if new := self.mapped(arg):
                    if new.startswith("SUPPRESS="):
                        continue
                    elif new == "SUPPRESS":
                        next(iter_args)
                        continue
                    arg = new
                if arg == self.numproc_flag:
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif arg.startswith(f"{self.numproc_flag}="):
                    i = len(self.numproc_flag) + 1
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
    from . import pluginmanager
    from .config import Config

    config = Config.from_defaults()
    launcher = pluginmanager.get_launcher(config=config)
    return launcher.submit(args, **kwargs)

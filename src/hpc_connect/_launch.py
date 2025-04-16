"""
Overview
--------

`hpc-launch` is a lightweight and configurable wrapper around program launchers like `mpiexec` or
`srun`. `hpc-launch` provides a single interface to multiple backends, simplifying the process of
launching applications in an HPC environment. `hpc-launch` passes all command line arguments to the
backend implementation.

Configuration
-------------

The default behavior of `hpc-launch` can be changed by providing a yaml configuration file.  The
default configuration is:

.. code-block:: yaml

   hpc_connect:
     launch:
       exec: mpiexec  # the launch backend.
       numproc_flag: -n  # Flag to pass to the backend before giving it the number of processors to run on.
       default_flags: []  # Flags to pass to the backend before any other arguments.
       mappings: {}  # Mapping of flag provided on the command line to flag passed to the backend

The configuration file is read at the first of:

1. ./hpc_connect.yaml
2. $HPCC_CONFIG_FILE
3. $XDG_CONFIG_HOME/hpc_connect/config.yaml
4. ~/.config/hpc_connect/config.yaml

Configuration settings can also be modified through the following environment variables:

* HPCC_LAUNCH_EXEC
* HPCC_LAUNCH_NUMPROC_FLAG
* HPCC_LAUNCH_DEFAULT_FLAGS
* HPCC_LAUNCH_MAPPINGS

Argument parsing
----------------

`hpc-launch` does not interpret or process any arguments passed to the backend, except for
arguments matching the `numproc_flag` configuration, which specifies the flag that indicates the
number of processors to be launched.

"""
import dataclasses
import json
import logging
import os
import shlex
import shutil
import subprocess
from typing import Any
from typing import Generator
from typing import Sequence

logger = logging.getLogger("hpc_connect")

import yaml

from . import pluginmanager


class Namespace:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []
        self.default_flags: list[str] = []

    def __len__(self) -> int:
        return len(self.specs)

    def __iter__(self) -> Generator[tuple[int | None, list[str]], None, None]:
        for i, spec in enumerate(self.specs):
            yield self.processes[i], spec

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)


class ArgumentParser:
    def __init__(self, *, mappings: dict[str, str] | None, numproc_flag: str = "-n") -> None:
        self.mappings: dict[str, str] = mappings or {}
        self.numproc_flag = numproc_flag
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

    def parse_args(self, args: Sequence[str]) -> Namespace:
        """Inspect arguments to launch to infer number of processors requested"""
        namespace = Namespace()
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
                namespace.add(spec, processes)
                spec.clear()
                command_seen, processes = False, None
            else:
                spec.append(arg)

        if spec:
            namespace.add(spec, processes)

        return namespace


@dataclasses.dataclass
class LaunchConfig:
    exec: str = "mpiexec"
    default_flags: list[str] = dataclasses.field(default_factory=list)
    numproc_flag: str = "-n"
    mappings: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        self.read_from_file()
        # Environment variables override file variables
        self.read_from_env()

        path = shutil.which(self.exec)
        if path is None:
            logger.warning(f"{self.exec}: executable not found")
        else:
            self.exec = path

    def read_from_file(self) -> None:
        file: str
        if os.path.exists("hpc_connect.yaml"):
            file = os.path.abspath("hpc_connect.yaml")
        elif "HPCC_CONFIG_FILE" in os.environ:
            file = os.environ["HPCC_CONFIG_FILE"]
        elif "XDG_CONFIG_HOME" in os.environ:
            file = os.path.join(os.environ["XDG_CONFIG_HOME"], "hpc_connect/config.yaml")
        else:
            file = os.path.expanduser("~/.config/hpc_connect/config.yaml")

        if not os.path.exists(file):
            return

        with open(file) as fh:
            config = yaml.safe_load(fh)
        fc = config["hpc_connect"].get("launch", {})
        if "exec" in fc:
            self.exec = fc["exec"]
        if "default_flags" in fc:
            self.default_flags = shlex.split(fc["default_flags"])
        if "numproc_flag" in fc:
            self.numproc_flag = fc["numproc_flag"]
        if "mappings" in fc:
            self.mappings.update(fc["mappings"])

    def read_from_env(self) -> None:
        if x := os.getenv("HPCC_LAUNCH_EXEC"):
            self.exec = x
        if x := os.getenv("HPCC_LAUNCH_DEFAULT_FLAGS"):
            self.default_flags = shlex.split(x)
        if x := os.getenv("HPCC_LAUNCH_NUMPROC_FLAG"):
            self.numproc_flag = x
        if x := os.getenv("HPCC_LAUNCH_MAPPINGS"):
            self.mappings.update(json.loads(x))


def join_args(args: Namespace, config: LaunchConfig | None = None) -> list[str]:
    config = config or LaunchConfig()
    cmd = pluginmanager.manager.hook.hpc_connect_launch_join_args(
        args=args, exec=config.exec, default_flags=config.default_flags
    )
    return cmd


def launch(args_in: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess:
    config = LaunchConfig()
    parser = ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
    args = parser.parse_args(args_in)
    cmd = join_args(args, config=config)
    return subprocess.run(cmd, **kwargs)

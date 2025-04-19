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
       local_options: []  # Flags to pass to the backend before any other arguments.
       default_options: []  # Flags to pass to the backend before any other arguments.
       mappings: {-n: <numproc_flag>}  # Mapping of flag provided on the command line to flag passed to the backend

The configuration file is read at the first of:

1. ./hpc_connect.yaml
2. $HPCC_CONFIG_FILE
3. $XDG_CONFIG_HOME/hpc_connect/config.yaml
4. ~/.config/hpc_connect/config.yaml

Configuration settings can also be modified through the following environment variables:

* HPCC_LAUNCH_EXEC
* HPCC_LAUNCH_NUMPROC_FLAG
* HPCC_LAUNCH_LOCAL_OPTIONS
* HPCC_LAUNCH_DEFAULT_OPTIONS
* HPCC_LAUNCH_MAPPINGS

Argument parsing
----------------

`hpc-launch` does not interpret or process any arguments passed to the backend, except for
arguments matching the `numproc_flag` configuration, which specifies the flag that indicates the
number of processors to be launched.

"""

import logging
import shutil
import subprocess
from typing import Any
from typing import Generator
from typing import Sequence

logger = logging.getLogger("hpc_connect")


from . import config
from . import pluginmanager


class Namespace:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []

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
        return None

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


def join_args(args: Namespace) -> list[str]:
    cmd = pluginmanager.manager.hook.hpc_connect_launch_join_args(
        args=args,
        exec=config.get("launch:exec"),
        local_options=config.get("launch:local_options"),
        global_options=config.get("launch:default_options"),
    )
    return cmd


def launch(args_in: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess:
    f = lambda p: config.get(p)
    parser = ArgumentParser(mappings=f("launch:mappings"), numproc_flag=f("launch:numproc_flag"))
    args = parser.parse_args(args_in)
    cmd = join_args(args)
    return subprocess.run(cmd, **kwargs)

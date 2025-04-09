# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import abc
import argparse
import getpass
import importlib
import math
import os
import shutil
import sys
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Any
from typing import Protocol
from typing import TextIO

from .util import cpu_count
from .util import make_template_env
from .util import sanitize_path
from .util import set_executable
from .util import time_in_seconds


class HPCProcess(Protocol):
    @property
    def returncode(self) -> int | None:
        raise NotImplementedError

    @returncode.setter
    def returncode(self, arg: int) -> None:
        raise NotImplementedError

    def poll(self) -> int | None:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError


class HPCConfig:
    def __init__(self) -> None:
        self.resources: list[dict] = [
            {
                "name": "node",
                "type": None,
                "count": 1,
                "partition": None,
                "resources": [
                    {
                        "name": "cpu",
                        "type": None,
                        "count": cpu_count(),
                    },
                ],
            }
        ]

    def set_resource_spec(self, arg: list[dict]) -> None:
        self.resources.clear()
        self.resources.extend(arg)

    def get_per_node(self, name: str) -> int | None:
        for resource in self.resources:
            if resource["name"] == "node":
                for child in resource["resources"]:
                    if child["name"] == name:
                        return child["count"]
        return None

    @property
    def cpus_per_node(self) -> int:
        return self.get_per_node("cpu") or 1

    @property
    def gpus_per_node(self) -> int:
        return self.get_per_node("gpu") or 0

    @property
    def node_count(self) -> int:
        for resource in self.resources:
            if resource["name"] == "node":
                return resource["count"]
        raise ValueError("Unable to determine node count")

    @property
    def cpu_count(self) -> int:
        return self.node_count * self.cpus_per_node

    @property
    def gpu_count(self) -> int:
        return self.node_count * self.gpus_per_node

    def nodes_required(self, max_cpus: int | None = None, max_gpus: int | None = None) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of as a single MPI
        rank"""
        nodes = max(1, int(math.ceil((max_cpus or 1) / self.cpus_per_node)))
        if self.gpus_per_node:
            nodes = max(nodes, int(math.ceil((max_gpus or 0) / self.gpus_per_node)))
        return nodes


class HPCBackend(ABC):
    name = "<backend>"

    def __init__(self) -> None:
        self.config = HPCConfig()

    @staticmethod
    @abstractmethod
    def matches(name: str) -> bool: ...

    @property
    def supports_subscheduling(self) -> bool:
        return False

    @property
    def polling_frequency(self) -> float:
        s = os.getenv("HPCC_POLLING_FREQUENCY") or 30.0  # 30s.
        return time_in_seconds(s)

    @abstractmethod
    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
        **kwargs: Any,
    ) -> HPCProcess: ...

    def submitn(
        self,
        name: list[str],
        args: list[list[str]],
        scriptname: list[str] | None = None,
        qtime: list[float] | None = None,
        submit_flags: list[list[str]] | None = None,
        variables: list[dict[str, str | None]] | None = None,
        output: list[str] | None = None,
        error: list[str] | None = None,
        nodes: list[int] | None = None,
        cpus: list[int] | None = None,
        gpus: list[int] | None = None,
        **kwargs: Any,
    ) -> HPCProcess:
        raise NotImplementedError(f"{self.name} backend has not implemented submitn")

    @property
    def submission_template(self) -> str:
        raise NotImplementedError

    def write_submission_script(
        self,
        name: str,
        args: list[str],
        scriptname: str | TextIO | None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
    ) -> str | None:
        template_dirs = {str(importlib.resources.files("hpc_connect").joinpath("templates"))}
        data = self.format_submission_data(
            name,
            args,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
        )
        template = self.submission_template
        if not os.path.exists(template):
            raise FileNotFoundError(template)
        d, f = os.path.split(os.path.abspath(template))
        template_dirs.add(d)
        env = make_template_env(*template_dirs)
        t = env.get_template(f)
        if hasattr(scriptname, "write"):
            scriptname.write(t.render(data))  # type: ignore
            return None
        scriptname = scriptname or unique_scriptname()
        file = sanitize_path(scriptname)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, "w") as fh:
            fh.write(t.render(data))
        set_executable(file)
        return file

    def format_submission_data(
        self,
        name: str,
        args: list[str],
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
    ) -> dict[str, Any]:
        if nodes is None and cpus is None:
            raise ValueError("must specify at least one of nodes and cpus")
        output = output or "stdout.txt"
        data: dict[str, Any] = {
            "name": name,
            "time": qtime or 1.0,
            "args": submit_flags or [],
            "nodes": nodes,
            "cpus": cpus,
            "gpus": gpus,
            "cpus_per_node": self.config.cpus_per_node,
            "gpus_per_node": self.config.gpus_per_node,
            "user": getpass.getuser(),
            "date": datetime.now().strftime("%c"),
            "variables": variables or {},
            "commands": args,
            "output": output,
            "error": error,
        }
        return data


class LaunchParser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        kwargs["add_help"] = False
        super().__init__(**kwargs)
        self.add_argument("--backend", help="Use this launcher backend [default: mpi]")
        self.add_argument("--config-file", help="Use this configuration file [default: None]")
        self.add_argument(
            "args", metavar="...", nargs=argparse.REMAINDER, help="Arguments to pass to launcher"
        )

    def preparse(self, argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
        namespace = argparse.Namespace(help=False, backend=None, config_file=None)
        remainder: list[str] = []
        iter_argv = iter(argv or sys.argv[1:])
        while True:
            try:
                arg = next(iter_argv)
            except StopIteration:
                break
            if arg.startswith(("--backend=", "--config-file=")):
                opt, _, value = arg.partition("=")
                setattr(namespace, opt[2:].replace("-", "_"), value)
            elif arg in ("--backend", "--config-file"):
                setattr(namespace, arg[2:].replace("-", "_"), next(iter_argv))
            elif arg in ("-h", "--help"):
                namespace.help = True
            else:
                remainder.append(arg)
                remainder.extend(iter_argv)
                break
        return namespace, remainder


class LaunchArgs:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)


class HPCLauncher(abc.ABC):
    name = "<launcher>"
    numproc_flags = ("-n", "--n", "-np", "--np", "-c")
    numproc_long_flags = ("--n=", "--np=")

    def __init__(self, *hints: str, config_file: str | None = None) -> None: ...

    @staticmethod
    @abstractmethod
    def matches(name: str) -> bool: ...

    @property
    @abc.abstractmethod
    def executable(self) -> str: ...

    def setup_parser(self, parser: LaunchParser) -> None:
        pass

    def default_args(self) -> list[str]:
        return []

    def inspect_args(self, args: list[str]) -> LaunchArgs:
        """Inspect arguments to launcher to infer number of processors requested"""
        la = LaunchArgs()

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
                if arg in self.numproc_flags:
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif arg.startswith(self.numproc_long_flags):
                    _, _, s = arg.partition("=")
                    processes = int(s)
                    spec.append(arg)
                else:
                    spec.append(arg)
            elif arg == ":":
                # MPMD: end of this segment
                la.add(spec, processes)
                spec.clear()
                command_seen, processes = False, None
            else:
                spec.append(arg)

        if spec:
            la.add(spec, processes)

        return la

    def format_program_args(self, args: LaunchArgs) -> list[str]:
        specs = list(self.default_args())
        for i, arg in enumerate(specs):
            specs[i] = arg % {"np": args.processes[i]}
        specs.extend(args.specs[0])
        for spec in args.specs[1:]:
            specs.append(":")
            specs.extend(spec)
        return specs


def unique_scriptname() -> str:
    template = "Submit-%d.sh"
    i = 1
    while True:
        script = template % i
        if not os.path.exists(script):
            return script
        i += 1


class HPCSubmissionFailedError(Exception):
    pass

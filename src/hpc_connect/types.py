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
from .util import partition
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


class Config:
    def __init__(self) -> None:
        self._cpus_per_node: int = cpu_count()
        self._gpus_per_node: int = 0
        self._node_count: int = 1
        self.set_from_environment()

    def set_from_environment(self) -> None:
        if val := os.getenv("HPC_CONNECT_CPUS_PER_NODE"):
            self.cpus_per_node = int(val)
        if val := os.getenv("HPC_CONNECT_GPUS_PER_NODE"):
            self.gpus_per_node = int(val)
        if val := os.getenv("HPC_CONNECT_NODE_COUNT"):
            self.node_count = int(val)

    @property
    def cpus_per_node(self) -> int:
        return self._cpus_per_node

    @cpus_per_node.setter
    def cpus_per_node(self, arg: int) -> None:
        if arg < 0:
            raise ValueError(f"cpus_per_node must be a positive integer ({arg} < 0)")
        self._cpus_per_node = int(arg)

    @property
    def gpus_per_node(self) -> int:
        return self._gpus_per_node

    @gpus_per_node.setter
    def gpus_per_node(self, arg: int) -> None:
        if arg < 0:
            raise ValueError(f"gpus_per_node must be a positive integer ({arg} < 0)")
        self._gpus_per_node = int(arg)

    @property
    def node_count(self) -> int:
        return self._node_count

    @node_count.setter
    def node_count(self, arg: int) -> None:
        if arg < 0:
            raise ValueError(f"node_count must be a positive integer ({arg} < 0)")
        self._node_count = int(arg)

    @property
    def cpu_count(self) -> int:
        return self.node_count * self.cpus_per_node

    @property
    def gpu_count(self) -> int:
        return self.node_count * self.gpus_per_node

    def nodes_required(self, tasks: int) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of as a single MPI
        rank"""
        tasks = tasks or 1
        if tasks < self.cpus_per_node:
            return 1
        nodes = int(math.ceil(tasks / self.cpus_per_node))
        return nodes


class HPCBackend(ABC):
    name = "<backend>"

    def __init__(self) -> None:
        self.config = Config()

    @staticmethod
    @abstractmethod
    def matches(name) -> bool: ...

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
        #
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
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
        #
        tasks: list[int] | None = None,
        cpus_per_task: list[int] | None = None,
        gpus_per_task: list[int] | None = None,
        tasks_per_node: list[int] | None = None,
        nodes: list[int] | None = None,
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
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
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
            tasks=tasks,
            cpus_per_task=cpus_per_task or 1,
            gpus_per_task=gpus_per_task or 0,
            tasks_per_node=tasks_per_node,
            nodes=nodes,
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
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
    ) -> dict[str, Any]:
        tasks = tasks or 1
        if nodes is None:
            nodes = self.config.nodes_required(tasks)
        output = output or "stdout.txt"
        data: dict[str, Any] = {
            "name": name,
            "time": qtime or 1.0,
            "args": submit_flags or [],
            "nodes": nodes,
            "tasks": tasks,
            "cpus_per_task": cpus_per_task or 1,
            "gpus_per_task": gpus_per_task or 0,
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
        self.add_argument("--backend", help="Use this launcher backend [default: mpiexec]")
        self.add_argument("--config-file", help="Use this configuration file")

    def preparse(
        self, argv: list[str] | None = None
    ) -> tuple[argparse.Namespace, list[str], list[str]]:
        l_opts, g_opts = partition(argv or sys.argv[1:], lambda x: x.startswith("-Wl,"))
        args, l_opts = self.parse_known_args([_[4:] for _ in l_opts])
        return args, l_opts, g_opts


class LaunchArgs:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []
        self.help: bool = False

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)

    def empty(self) -> bool:
        return len(self.specs) == 0


class HPCLauncher(abc.ABC):
    name = "<launcher>"

    def __init__(self, *hints: str, config_file: str | None = None) -> None:
        pass

    @property
    @abc.abstractmethod
    def executable(self) -> str: ...

    @classmethod
    @abc.abstractmethod
    def factory(self, arg: str, config_file: str | None = None) -> "HPCLauncher | None": ...

    def setup_parser(self, parser: LaunchParser) -> None:
        pass

    def set_main_options(self, args: argparse.Namespace) -> None:
        pass

    def default_args(self) -> list[str]:
        return []

    def inspect_args(self, args: list[str]) -> LaunchArgs:
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
                if la.empty() and arg in ("-h", "--help"):
                    la.help = True
                if arg in ("-n", "--n", "-np", "--np", "-c"):
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif arg.startswith(("--n=", "--np=")):
                    arg, _, s = arg.partition("=")
                    processes = int(s)
                    spec.extend([arg, s])
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

import math
import os
import shlex
import shutil
from abc import ABC
from abc import abstractmethod
from typing import Optional
from typing import TextIO

from .util import cpu_count


class HPCScheduler(ABC):
    """Setup and submit jobs to an HPC scheduler"""

    shell = "/bin/sh"
    name = "<none>"
    command_name = "<submit-command>"

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
            """Nodes required to run ``tasks`` tasks.  A task can be thought of a single MPI rank"""
            nodes = int(math.ceil(tasks / self.cpus_per_node))
            return nodes if nodes <= self.node_count else -1

    def __init__(self) -> None:
        command = shutil.which(self.command_name)
        if command is None:
            raise ValueError(f"{self.command_name} not found on PATH")
        self.exe: str = command
        self.config = HPCScheduler.Config()
        self.default_args = self.read_default_args()

    def add_default_args(self, *args: str) -> None:
        self.default_args.extend(args)

    def nodes_required(self, tasks: int) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of a single MPI rank"""
        return self.config.nodes_required(tasks)

    @staticmethod
    @abstractmethod
    def matches(name: Optional[str]) -> bool: ...

    @abstractmethod
    def write_submission_script(
        self,
        script: list[str],
        file: TextIO,
        *,
        tasks: int,
        nodes: Optional[int] = None,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
        qtime: Optional[float] = None,
        variables: Optional[dict[str, Optional[str]]] = None,
    ) -> None: ...

    @abstractmethod
    def submit_and_wait(
        self,
        script: str,
        *,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None: ...

    def read_default_args(self) -> list[str]:
        default_args: list[str] = []
        if default_envargs := os.getenv("HPC_CONNECT_DEFAULT_SCHEDULER_ARGS"):
            default_args.extend(shlex.split(default_envargs))
        if envargs := os.getenv("HPC_CONNECT_SCHEDULER_ARGS"):
            default_args.extend(shlex.split(envargs))
        return default_args

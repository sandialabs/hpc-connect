import logging
import math
import os
import shlex
import time
from abc import ABC
from abc import abstractmethod
from typing import Optional
from typing import TextIO
from typing import Union

from .util import cpu_count

logger = logging.getLogger("hpc_connect")


class HPCProcess(ABC):
    def __init__(
        self,
        script: str,
        *,
        job_name: Optional[str] = None,
    ) -> None:
        self.script = script
        self.job_name = job_name
        self.stdout: Optional[Union[TextIO, int]] = None
        self.stderr: Optional[Union[TextIO, int]] = None
        self.returncode: Optional[int] = None

    @abstractmethod
    def poll(self) -> Optional[int]:
        """Check if child process has terminated. Set and return returncode attribute. Otherwise, returns None."""

    @abstractmethod
    def cancel(self) -> None:
        """Stop the child process."""


class HPCScheduler(ABC):
    """Setup and submit jobs to an HPC scheduler"""

    shell = "/bin/sh"
    name = "<none>"

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
            nodes = int(math.ceil(tasks / self.cpus_per_node))
            return nodes if nodes <= self.node_count else -1

    def __init__(self) -> None:
        self.config = HPCScheduler.Config()
        self.default_args = self.read_default_args()

    def add_default_args(self, *args: str) -> None:
        """Add default arguments to the scheduler submission command line"""
        self.default_args.extend(args)

    def read_default_args(self) -> list[str]:
        """Read default arguments from the command line"""
        default_args: list[str] = []
        if default_envargs := os.getenv("HPC_CONNECT_DEFAULT_SCHEDULER_ARGS"):
            default_args.extend(shlex.split(default_envargs))
        if envargs := os.getenv("HPC_CONNECT_SCHEDULER_ARGS"):
            default_args.extend(shlex.split(envargs))
        return default_args

    def nodes_required(self, tasks: int) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of a single MPI rank"""
        return self.config.nodes_required(tasks)

    @staticmethod
    @abstractmethod
    def matches(name: Optional[str]) -> bool:
        """Is this the scheduler for ``name``?"""

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
    ) -> None:
        """Write a submission script that is compatible with ``submit_and_wait`` and ``submit``"""

    @abstractmethod
    def submit(self, script: str, *, job_name: Optional[str] = None) -> HPCProcess:
        """Submit ``script`` to the scheduler"""

    def submit_and_wait(
        self,
        script: str,
        *,
        job_name: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Optional[int]:
        """Submit ``script`` to the scheduler and wait for it to return"""
        timeout = timeout or -1.0
        start = time.monotonic()
        proc = self.submit(script, job_name=job_name)
        try:
            # give the process time to get in the queue
            time.sleep(1)
            while True:
                if proc.poll() is not None:
                    break
                if timeout > 0 and time.monotonic() - start > timeout:
                    proc.cancel()
                    break
                time.sleep(0.5)
        except BaseException as e:
            proc.cancel()
            if isinstance(e, KeyboardInterrupt):
                return None
            raise
        finally:
            if hasattr(proc.stdout, "fileno"):
                proc.stdout.close()  # type: ignore
            if hasattr(proc.stderr, "fileno"):
                proc.stderr.close()  # type: ignore
        return proc.returncode


class HPCSubmissionFailedError(Exception):
    pass

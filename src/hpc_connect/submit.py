import logging
import math
import multiprocessing
import os
import shlex
import signal
import time
from abc import ABC
from abc import abstractmethod
from typing import TextIO

from .job import Job
from .util import cpu_count

logger = logging.getLogger("hpc_connect")


class HPCProcess(ABC):
    def __init__(
        self,
        *,
        job: Job,
    ) -> None:
        self.job = job
        self.stdout: TextIO | int | None = None
        self.stderr: TextIO | int | None = None
        self.returncode: int | None = None

    @abstractmethod
    def poll(self) -> int | None:
        """Check if child process has terminated. Set and return returncode attribute. Otherwise, returns None."""

    @abstractmethod
    def cancel(self, returncode: int) -> None:
        """Stop the child process. Set the return code to the specified value."""

    def finalize(self) -> None:
        """Perform any necessary cleanup after a job has finished running."""
        if hasattr(self.stdout, "fileno"):
            self.stdout.close()  # type: ignore
        if hasattr(self.stderr, "fileno"):
            self.stderr.close()  # type: ignore
        self.job.returncode = self.returncode


class HPCScheduler(ABC):
    """Setup and submit jobs to an HPC scheduler"""

    shell = "/bin/sh"
    name = "<none>"
    lock = multiprocessing.RLock()

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

    def __init__(self) -> None:
        self.config = HPCScheduler.Config()
        self.default_args = self.read_default_args()
        self.processes: list[HPCProcess] = list()
        self._signaled = False

    @property
    def supports_subscheduling(self) -> bool:
        return False

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

    def nodes_required(self, tasks: int | None) -> int:
        """Nodes required to run ``tasks`` tasks.  A task can be thought of a single MPI rank"""
        return self.config.nodes_required(tasks or 1)

    @staticmethod
    @abstractmethod
    def matches(name: str | None) -> bool:
        """Is this the scheduler for ``name``?"""

    @abstractmethod
    def write_submission_script(self, job: Job, file: TextIO) -> None:
        """Write a submission script that is compatible with ``submit_and_wait`` and ``submit``"""

    @abstractmethod
    def submit(self, job: Job) -> HPCProcess:
        """Submit ``job`` to the scheduler."""

    def shutdown(self, returncode: int = 0) -> None:
        """Perform any post-run cleanup"""
        self.cancel(returncode)

    def poll(self) -> None:
        """Poll the status of running processes and finalize any that have finished"""
        for proc in self.processes:
            if proc.returncode is not None:
                continue
            elif proc.poll() is not None:
                proc.finalize()

    def cancel(self, returncode: int) -> None:
        """Cancel any processes that have not completed"""
        with self.lock:
            for proc in self.processes:
                if proc.returncode is None:
                    proc.cancel(returncode)
                proc.finalize()

    def wait(self, timeout: float, poll_frequency: float) -> None:
        """Wait for running processes to complete"""
        start = time.monotonic()
        try:
            while any(proc.returncode is None for proc in self.processes):
                if timeout > 0 and time.monotonic() - start > timeout:
                    raise TimeoutError
                self.poll()
                time.sleep(poll_frequency)
        except BaseException as e:
            logging.error(f"Got exception, cancelling jobs: {e}")
            returncode = 66 if isinstance(e, TimeoutError) else 1
            self.cancel(returncode)
            if isinstance(e, KeyboardInterrupt):
                return None
            raise

    def submit_and_wait(
        self,
        *jobs: Job,
        sequential: bool = True,
        timeout: float | None = None,
        poll_frequency=0.5,
    ) -> None:
        """Submit ``jobs`` to the scheduler and wait for it to return"""

        def cancel_jobs(sig, frame) -> None:
            with self.lock:
                if not self._signaled:
                    self._signaled = True
                    logger.error(
                        f"Caught signal {sig}, canceling jobs and shutting down scheduler ({self.name})."
                    )
                self.shutdown(1)

        signal.signal(signal.SIGINT, cancel_jobs)
        signal.signal(signal.SIGTERM, cancel_jobs)

        timeout = timeout or -1.0
        if sequential:
            for job in jobs:
                self.processes = [self.submit(job)]
                time.sleep(1)  # wait for the process to start
                self.wait(timeout, poll_frequency)
        else:
            for job in jobs:
                self.processes.append(self.submit(job))

            time.sleep(1)  # wait for the processes to start
            self.wait(timeout, poll_frequency)
        self.shutdown()
        return


class HPCSubmissionFailedError(Exception):
    pass

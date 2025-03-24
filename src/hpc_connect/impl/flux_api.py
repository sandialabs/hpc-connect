import importlib.resources
import logging
import math
import multiprocessing
import multiprocessing.synchronize
import os
import time
from concurrent.futures import CancelledError
from typing import Any

import flux  # type: ignore
from flux import Flux  # type: ignore
from flux.job import FluxExecutor  # type: ignore
from flux.job import FluxExecutorFuture  # type: ignore
from flux.job import Jobspec  # type: ignore
from flux.job import JobspecV1  # type: ignore

from ..hookspec import hookimpl
from ..types import HPCBackend
from ..types import HPCProcess
from ..types import HPCSubmissionFailedError

logger = logging.getLogger("hpc_connect")


class FluxProcess(HPCProcess):
    JOB_TIMEOUT_CODE = 66

    def __init__(self, name: str, future: FluxExecutorFuture) -> None:
        self.fh = Flux()
        self.name = name
        self.fut: FluxExecutorFuture = future
        self.jobid: int | None = None
        self._rc: int | None = None

        def set_returncode(fut: FluxExecutorFuture):
            try:
                info = flux.job.result(self.fh, fut.jobid())
                self.returncode = info.returncode
            except (CancelledError, Exception):
                self.returncode = 1

        def set_jobid(fut: FluxExecutorFuture):
            try:
                self.jobid = fut.jobid()
                logger.info(f"submitted job {self.jobid} for {self.name}")
            except CancelledError:
                self.returncode = 1
            except Exception as e:
                raise HPCSubmissionFailedError(e)

        self.fut.add_jobid_callback(set_jobid)
        self.fut.add_done_callback(set_returncode)

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        return self.returncode

    def cancel(self) -> None:
        logger.info(f"Canceling job {self.jobid}.")
        try:
            flux.job.cancel(self.fh, self.jobid)
        except OSError:
            logger.debug(f"Job {self.jobid} is inactive, cannot cancel.")
        except Exception:
            logger.error(f"Failed to cancel job {self.jobid}.")
        self.returncode = 1


class FluxMultiProcess(HPCProcess):
    def __init__(
        self,
        lock: multiprocessing.synchronize.RLock,
        procs: list[FluxProcess] | None = None,
    ) -> None:
        self.lock = lock
        self.procs = procs or []

    @property
    def returncode(self) -> int | None:
        rcs = [p.returncode for p in self.procs if p is not None]
        if not rcs:
            return None
        return max(rcs)  # type: ignore

    @returncode.setter
    def returncode(self, arg: int) -> None:
        raise NotImplementedError

    def append(self, proc: FluxProcess) -> None:
        self.procs.append(proc)

    def pop(self, /, i: int = -1) -> FluxProcess:
        return self.procs.pop(i)

    def cancel(self) -> None:
        with self.lock:
            for proc in self.procs:
                proc.cancel()

    def poll(self) -> int | None:
        stat: list[int | None] = []
        for proc in self.procs:
            stat.append(proc.poll())
        if any([_ is None for _ in stat]):
            return None
        return max(stat)  # type: ignore


class FluxBackend(HPCBackend):
    """Setup and submit jobs to the Flux scheduler"""

    name = "flux"
    lock: multiprocessing.synchronize.RLock = multiprocessing.RLock()

    def __init__(self) -> None:
        super().__init__()
        self.flux: FluxExecutor | None = FluxExecutor()
        self.fh = Flux()

    #    def __del__(self) -> None:
    #        self.shutdown()

    @property
    def supports_subscheduling(self) -> bool:
        return True

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() == "flux"

    @property
    def submission_template(self) -> str:
        if "HPCC_FLUX_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_FLUX_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpc_connect").joinpath("templates/flux.sh.in"))

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
        exclusive: bool = True,
    ) -> FluxProcess:
        # the Flux submission script is not used, but we write it for inspection, if needed
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            tasks=tasks,
            cpus_per_task=cpus_per_task,
            gpus_per_task=gpus_per_task,
            tasks_per_node=tasks_per_node,
            nodes=nodes,
        )
        assert script is not None
        jobspec = self.create_jobspec(
            name,
            script,
            qtime=qtime,
            variables=variables,
            output=output,
            error=error,
            tasks=tasks,
            cpus_per_task=cpus_per_task,
            gpus_per_task=gpus_per_task,
            tasks_per_node=tasks_per_node,
            nodes=nodes,
            exclusive=exclusive,
        )
        fut = self.flux.submit(jobspec)  # type: ignore
        return FluxProcess(name, future=fut)

    def create_jobspec(
        self,
        name: str,
        script: str,
        qtime: float | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
        exclusive: bool = True,
    ) -> Jobspec:
        """
        Create the flux jobspec for the executable script ``script``.  The number of tasks
        ``tasks`` is the number of MPI tasks that ``script`` launches, not the number of
        copies of ``script`` that flux should launch.

        """
        tasks = tasks or 1
        cores_per_task = tasks * (cpus_per_task or 1)
        gpus_per_task = tasks * (gpus_per_task or 0)
        jobspec = JobspecV1.from_command(
            command=[script],
            num_tasks=1,
            num_nodes=nodes or self.config.nodes_required(cores_per_task),
            cores_per_task=cores_per_task,
            gpus_per_task=gpus_per_task,
            exclusive=exclusive,
        )
        jobspec.setattr("system.job.name", name)
        jobspec.stdout = output or "job-ouput.txt"
        jobspec.stderr = error or output or "job-error.txt"
        jobspec.duration = time_limit_in_seconds(qtime, pad=60)
        env = os.environ.copy()
        if variables:
            for key, val in variables.items():
                if val is None:
                    env.pop(key, None)
                else:
                    env[key] = val
        jobspec.environment = env
        return jobspec

    def shutdown(self):
        with self.lock:
            if self.flux:
                time.sleep(0.25)
                if flux.job:
                    jobs = flux.job.JobList(self.fh, filters=["running", "pending"]).jobs()
                    if jobs:
                        logger.warning(f"{len(jobs)} active jobs remain after shutdown")
                self.flux.shutdown(wait=False, cancel_futures=True)
                self.flux = None

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
    ) -> FluxMultiProcess:
        if not os.getenv("HPC_CONNECT_ENABLE_FLUX_SUBMITN"):
            raise NotImplementedError(
                "canary seems to lock up when running submitn for all but trivial tests. "
                "This will need to be debugged and fixed. The problem may be in our use of "
                "JobspecV1.from_command *or* it could be an issue with file locking in "
                "canary on some file systems.  Set the HPC_CONNECT_ENABLE_FLUX_SUBMITN "
                "environment variable to bypass this check"
            )
        assert len(name) == len(args)
        procs = FluxMultiProcess(self.lock)
        with self.lock:
            for i in range(len(name)):
                proc = self.submit(
                    name[i],
                    args[i],
                    select(scriptname, i, f"flux-submit-{i}"),
                    qtime=select(qtime, i),
                    submit_flags=select(submit_flags, i),
                    variables=select(variables, i),
                    output=select(output, i, f"flux-out-{i}"),
                    error=select(output, i, f"flux-err-{i}"),
                    tasks=select(tasks, i),
                    cpus_per_task=select(cpus_per_task, i, 1),
                    gpus_per_task=select(gpus_per_task, i, 0),
                    tasks_per_node=select(tasks_per_node, i),
                    nodes=select(nodes, i),
                    exclusive=False,
                )
                procs.append(proc)
        return procs


def select(arg: Any, i: int, default: Any = None) -> Any:
    if isinstance(arg, list):
        return arg[i]
    return arg or default


def time_limit_in_seconds(qtime: float | None, pad: int = 0) -> int:
    """Return the time limit in seconds. Guarenteed return value >= 1."""
    limit = math.ceil(qtime or 1)
    if pad > 0:
        limit += pad
    return limit


@hookimpl
def hpc_connect_backend():
    return FluxBackend

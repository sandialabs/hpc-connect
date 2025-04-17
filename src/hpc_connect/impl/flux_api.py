# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from datetime import timedelta
import importlib.resources
import logging
import math
import multiprocessing
import multiprocessing.synchronize
import os
import subprocess
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
from ..util import time_in_seconds

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
                logger.debug(f"submitted job {self.jobid} for {self.name}")
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
        logger.warning(f"Canceling flux job {self.jobid}")
        try:
            flux.job.cancel(self.fh, self.jobid)
        except OSError:
            logger.debug(f"Job {self.jobid} is inactive, cannot cancel")
        except Exception:
            logger.error(f"Failed to cancel job {self.jobid}")
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


def parse_resource_info(output: str) -> dict[str, int] | None:
    """Parses the output from `flux resource info` and returns a dictionary of resource values.

    The expected output format is "1 Nodes, 32 Cores, 1 GPUs".

    Returns:
        dict: A dictionary containing the resource values with the following keys:
            - nodes (int): The number of nodes.
            - cpu (int): The number of CPU cores.
            - gpu (int): The number of GPU devices.
    """
    parts = output.split(", ")
    vals = [int(p.split()[0]) for p in parts]
    if len(vals) != 3:
        return None
    return {"nodes": vals[0], "cpu": vals[1], "gpu": vals[2]}


def read_resource_info() -> dict[str, Any] | None:
    try:
        output = subprocess.check_output(["flux", "resource", "info"], encoding="utf-8")
    except subprocess.CalledProcessError:
        return None
    if totals := parse_resource_info(output):
        # assume homogenous resources
        nodes = totals["nodes"]
        info: dict = {"name": "node", "type": None, "count": nodes}
        resources = info.setdefault("resources", [])
        resources.append({"name": "cpu", "type": None, "count": int(totals["cpu"] / nodes)})
        resources.append({"name": "gpu", "type": None, "count": int(totals["gpu"] / nodes)})
        return info
    return None


class FluxBackend(HPCBackend):
    """Setup and submit jobs to the Flux scheduler"""

    name = "flux"
    lock: multiprocessing.synchronize.RLock = multiprocessing.RLock()

    def __init__(self) -> None:
        super().__init__()
        self.flux: FluxExecutor | None = FluxExecutor()
        self.fh = Flux()
        if info := read_resource_info():
            self.config.set_resource_spec([info])

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

    def create_jobspec(
        self,
        name: str,
        script: str,
        duration: float | timedelta | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        cpus: int | None = None,
        gpus: int | None = None,
        nodes: int | None = None,
        exclusive: bool = True,
    ) -> Jobspec:
        """
        Create the flux jobspec for the executable script ``script``.  The number of tasks
        ``tasks`` is the number of MPI tasks that ``script`` launches, not the number of
        copies of ``script`` that flux should launch.

        """
        kwds: dict[str, Any] = {"command": [script], "exclusive": exclusive}
        alloc = self.get_alloc_settings(cpus, gpus, nodes)
        kwds.update(alloc)
        jobspec = JobspecV1.from_nest_command(**kwds)
        jobspec.setattr("system.job.name", name)
        jobspec.stdout = output or "job-ouput.txt"
        jobspec.stderr = error or output or "job-error.txt"
        jobspec.duration = duration or 60.0 # duration is in seconds
        env = os.environ.copy()
        if variables:
            for key, val in variables.items():
                if val is None:
                    env.pop(key, None)
                else:
                    env[key] = val
        jobspec.environment = env
        return jobspec

    def get_alloc_settings(
        self,
        cpus: int | None = None,
        gpus: int | None = None,
        nodes: int | None = None,
    ) -> dict[str, Any]:
        alloc: dict[str, Any] = {}
        if nodes is not None:
            if cpus is None:
                cpus = nodes*self.config.cpus_per_node
            if gpus is None:
                gpus = nodes*self.config.gpus_per_node
        else:
            cpus = cpus or 1
            gpus = gpus or 0
            nodes = self.config.nodes_required(max_cpus=cpus, max_gpus=gpus)
        
        alloc["num_nodes"] = nodes
        alloc["num_slots"] = nodes
        if nodes > 1:
            cpus = max(1, math.ceil(cpus / nodes))
            gpus = max(0, math.ceil(gpus / nodes))
            
        alloc["cores_per_slot"] = cpus
        alloc["gpus_per_slot"] = gpus
        return alloc

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
        exclusive: bool = True,
        **kwargs: Any,
    ) -> FluxProcess:
        cpus = cpus or kwargs.get("tasks")
        duration = timedelta(seconds=time_limit_in_seconds(qtime, pad=60))
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=max(1, duration.total_seconds()/60.0), # time limit in minutes in the script
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
        )
        assert script is not None
        jobspec = self.create_jobspec(
            name,
            script,
            duration=duration,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
            exclusive=exclusive,
        )
        fut = self.flux.submit(jobspec)  # type: ignore
        return FluxProcess(name, future=fut)

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
    ) -> FluxMultiProcess:
        cpus = cpus or kwargs.get("tasks")  # backward compatible
        assert len(name) == len(args)
        procs = FluxMultiProcess(self.lock)
        submission_delay: float = 0.0
        if t := os.getenv("HPC_CONNECT_FLUX_SUBMITN_DELAY"):
            submission_delay = time_in_seconds(t)
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
                    error=select(error, i, f"flux-err-{i}"),
                    nodes=select(nodes, i),
                    cpus=select(cpus, i),
                    gpus=select(gpus, i),
                    exclusive=False,
                )
                if submission_delay:
                    time.sleep(submission_delay)
                procs.append(proc)
        return procs

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
        data = super().format_submission_data(
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
        alloc = self.get_alloc_settings(cpus=cpus, gpus=gpus, nodes=nodes)
        data.update(alloc)
        return data


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

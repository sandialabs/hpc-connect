# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import math
import multiprocessing
import multiprocessing.synchronize
import os
import shutil
import time
from datetime import timedelta
from typing import Any

import flux  # type: ignore
from flux import Flux  # type: ignore
from flux.job import FluxExecutor  # type: ignore
from flux.job import Jobspec  # type: ignore
from flux.job import JobspecV1  # type: ignore

import hpc_connect
from hpc_connect.mpi import MPIExecAdapter
from hpc_connect.util import time_in_seconds

from .discover import read_resource_info
from .process import FluxProcess

logger = logging.getLogger("hpc_connect.flux.backend_api")


class FluxBackend(hpc_connect.Backend):
    name = "flux"

    def __init__(self, config: hpc_connect.Config | None = None) -> None:
        super().__init__(config=config)
        self._resource_specs: list[dict]
        if info := read_resource_info():
            self._resource_specs = [info]
        else:
            raise ValueError("Unable to determine system configuration from flux")
        self.flux: FluxExecutor | None = FluxExecutor()
        self.fh = Flux()

    @property
    def resource_specs(self) -> list[dict]:
        return self._resource_specs

    def supports_subscheduling(self) -> bool:
        return True

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        config = self.config.submit.resolve("flux")
        return hpc_connect.HPCSubmissionManager(adapter=FluxAdapter(backend=self, config=config))

    def launcher(self) -> hpc_connect.HPCLauncher:
        return hpc_connect.HPCLauncher(
            adapter=MPIExecAdapter(backend=self, config=self.config.launch.resolve("mpiexec"))
        )


class FluxAdapter:
    lock: multiprocessing.synchronize.RLock = multiprocessing.RLock()

    def __init__(self, backend: FluxBackend, config: hpc_connect.SubmitConfig) -> None:
        self.config = config
        self.backend = backend

    def poll_interval(self) -> float:
        s = os.getenv("HPCC_POLL_INTERVAL") or 30.0
        return time_in_seconds(s)

    def submit(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> FluxProcess:
        jobspec = self.prepare(spec, exclusive=exclusive)
        fut = self.backend.flux.submit(jobspec)  # type: ignore
        return FluxProcess(spec.name, future=fut)

    def prepare(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> Jobspec:
        duration = timedelta(seconds=time_limit_in_seconds(spec.time_limit, pad=60))
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        alloc = self.get_alloc_settings(spec.cpus, spec.gpus, spec.nodes)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            fh.write(f"#FLUX --nodes={spec.nodes}\n")
            fh.write(f"#FLUX --nslots={alloc['num_slots']}\n")
            fh.write(f"#FLUX --cores-per-slot={alloc['cores_per_slot']}\n")
            fh.write(f"#FLUX --gpus-per-slot={alloc['gpus_per_slot']}\n")
            fh.write(f"#FLUX: --time-limit={duration}")
            if spec.output:
                fh.write(f"#FLUX --output={spec.output}\n")
            if spec.error:
                fh.write(f"#FLUX --error={spec.output}\n")
            for arg in self.config.default_options:
                fh.write(f"#FLUX {arg}\n")
            for arg in spec.submit_args:
                fh.write(f"#FLUX {arg}\n")
            for var, val in spec.env.items():
                if val is None:
                    fh.write(f"unset {var}\n")
                else:
                    fh.write(f'export {var}="{val}"\n')
            for command in spec.commands:
                fh.write(f"{command}\n")
        os.chmod(script, 0o755)
        kwds: dict[str, Any] = {"command": [str(script)], "exclusive": exclusive}
        kwds.update(alloc)
        jobspec = JobspecV1.from_nest_command(**kwds)
        jobspec.setattr("system.job.name", spec.name)
        jobspec.stdout = spec.output or "job-ouput.txt"
        jobspec.stderr = spec.error or spec.output or "job-error.txt"
        jobspec.duration = duration or 60.0  # duration is in seconds
        env = os.environ.copy()
        if spec.env:
            for key, val in spec.env.items():
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
                cpus = nodes * self.backend.count_per_node("cpu")
            if gpus is None:
                gpus = nodes * self.backend.count_per_node("gpu", default=0)
        else:
            cpus = cpus or 1
            gpus = gpus or 0
            nodes = self.backend.nodes_required(max_cpus=cpus, max_gpus=gpus)

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
            if self.backend.flux:
                time.sleep(0.25)
                if flux.job:
                    jobs = flux.job.JobList(self.backend.fh, filters=["running", "pending"]).jobs()
                    if jobs:
                        logger.warning(f"{len(jobs)} active jobs remain after shutdown")
                self.backend.flux.shutdown(wait=False, cancel_futures=True)
                self.backend.flux = None


def time_limit_in_seconds(qtime: float | None, pad: int = 0) -> int:
    """Return the time limit in seconds. Guarenteed return value >= 1."""
    limit = math.ceil(qtime or 1)
    if pad > 0:
        limit += pad
    return limit

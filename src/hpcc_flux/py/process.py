# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import multiprocessing
import multiprocessing.synchronize
import time
from concurrent.futures import CancelledError
from typing import TYPE_CHECKING

import flux
import flux.job
from flux.job import FluxExecutorFuture

if TYPE_CHECKING:
    from flux.core.handle import Flux as FluxHandle

import hpc_connect

logger = logging.getLogger("hpc_connect.flux.py.process")


class FluxProcess(hpc_connect.HPCProcess):
    JOB_TIMEOUT_CODE = 66

    def __init__(self, name: str, future: FluxExecutorFuture, handle: "FluxHandle") -> None:
        self.handle = handle
        self.name = name
        self.fut: FluxExecutorFuture = future
        self._rc: int | None = None
        self.flux_jobid: flux.job.JobID | None = None

        def set_returncode(fut: FluxExecutorFuture):
            try:
                info = flux.job.result(self.handle, fut.jobid())
                self.returncode = info.returncode
            except (CancelledError, Exception):
                self.returncode = 1

        def set_proc_info(fut: FluxExecutorFuture):
            job = flux.job.get_job(self.handle, flux.job.JobID(self.jobid))
            self.completion_info = dict(job)

        def set_jobid(fut: FluxExecutorFuture):
            try:
                self.flux_jobid = fut.jobid()
                self.jobid = str(self.flux_jobid)
                logger.debug(f"submitted job {self.jobid} for {self.name}")
            except (CancelledError, Exception):
                self.returncode = 1

        def set_submittime(fut: FluxExecutorFuture, *args):
            self.submitted = time.time()

        def set_starttime(fut: FluxExecutorFuture, *args):
            self.started = time.time()

        self.fut.add_jobid_callback(set_jobid)
        self.fut.add_done_callback(set_returncode)
        self.fut.add_done_callback(set_proc_info)
        self.fut.add_event_callback("submit", set_submittime)
        self.fut.add_event_callback("start", set_starttime)

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
        if self.flux_jobid is not None:
            try:
                flux.job.cancel(self.handle, self.flux_jobid)
            except OSError:
                logger.debug(f"Job {self.jobid} is inactive, cannot cancel")
            except Exception:
                logger.error(f"Failed to cancel job {self.jobid}")
        self.returncode = 1


class FluxMultiProcess(hpc_connect.HPCProcess):
    def __init__(
        self, lock: multiprocessing.synchronize.RLock, procs: list[FluxProcess] | None = None
    ) -> None:
        self.lock = lock
        self.procs = procs or []

    @property
    def returncode(self) -> int | None:
        rcs = [p.returncode for p in self.procs if p is not None]
        if not rcs:
            return None
        return max(rcs)

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
        returncodes: list[int] = []
        pending = False
        for proc in self.procs:
            if (rc := proc.poll()) is None:
                pending = True
            else:
                returncodes.append(rc)
        return None if pending else max(returncodes)

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import multiprocessing
import multiprocessing.synchronize
import time
from concurrent.futures import CancelledError

import flux  # type: ignore
from flux import Flux  # type: ignore
from flux.job import FluxExecutorFuture  # type: ignore

import hpc_connect

logger = logging.getLogger("hpc_connect.flux.submit")


class FluxProcess(hpc_connect.HPCProcess):
    JOB_TIMEOUT_CODE = 66

    def __init__(self, name: str, future: FluxExecutorFuture) -> None:
        self.fh = Flux()
        self.name = name
        self.fut: FluxExecutorFuture = future
        self._rc: int | None = None

        def set_returncode(fut: FluxExecutorFuture):
            try:
                info = flux.job.result(self.fh, fut.jobid())
                self.returncode = info.returncode
            except (CancelledError, Exception):
                self.returncode = 1

        def set_jobid(fut: FluxExecutorFuture):
            try:
                self.jobid = str(fut.jobid())
                logger.debug(f"submitted job {self.jobid} for {self.name}")
            except CancelledError:
                self.returncode = 1
            except Exception as e:
                logger.exception("Submission failed")
                raise

        def set_submittime(fut: FluxExecutorFuture, *args):
            self.submitted = time.time()

        def set_starttime(fut: FluxExecutorFuture, *args):
            self.started = time.time()

        self.fut.add_jobid_callback(set_jobid)
        self.fut.add_done_callback(set_returncode)
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
        try:
            flux.job.cancel(self.fh, int(self.jobid))
        except OSError:
            logger.debug(f"Job {self.jobid} is inactive, cannot cancel")
        except Exception:
            logger.error(f"Failed to cancel job {self.jobid}")
        self.returncode = 1


class FluxMultiProcess(hpc_connect.HPCProcess):
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

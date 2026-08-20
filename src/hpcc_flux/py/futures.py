# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import time
from typing import Any
from typing import Callable

from .process import FluxProcess

logger = logging.getLogger("hpc_connect.flux.py.future")


class FluxFuture:
    """
    Threadless Future-like wrapper around a FluxProcess.

    This deliberately does not subclass or instantiate hpc_connect.Future,
    because hpc_connect.Future starts one polling thread per job.

    It implements the same interface expected by hpc_connect clients:
      - done()
      - cancelled()
      - cancel()
      - result(timeout)
      - proc_info(timeout)
      - add_done_callback(fn)
      - add_jobstart_callback(fn)
      - add_jobid_callback(fn)
      - jobid
      - returncode
    """

    def __init__(self, proc: FluxProcess, polling_interval: float = 1.0) -> None:
        self.proc = proc
        self._polling_interval = polling_interval or 1.0
        self._cancelled = False

    @property
    def jobid(self) -> str:
        return self.proc.jobid

    @property
    def returncode(self) -> int | None:
        return self.proc.returncode

    def done(self) -> bool:
        return self.proc.poll() is not None

    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> bool:
        if self.done():
            return False

        self._cancelled = True
        try:
            self.proc.cancel()
        except Exception:
            logger.debug("Failed to cancel Flux job %s", self.proc.jobid, exc_info=True)

        return True

    def result(self, timeout: float | None = None) -> int:
        deadline = None if timeout is None else time.monotonic() + timeout

        while not self.done():
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(f"Job {self.proc.jobid} did not finish in time")
            time.sleep(self._polling_interval)

        rc = self.proc.returncode
        return 1 if not isinstance(rc, int) else rc

    def proc_info(self, timeout: float | None = None) -> dict[str, Any]:
        self.result(timeout=timeout)
        info = getattr(self.proc, "completion_info", None)
        if info is None:
            return {}
        return dict(info)

    def add_done_callback(self, fn: Callable[["FluxFuture"], None]) -> None:
        if self.done():
            self._safeexec(fn)
            return

        try:
            self.proc.fut.add_done_callback(lambda _f: self._safeexec(fn))
        except Exception:
            logger.debug("Failed to add Flux done callback", exc_info=True)

    def add_jobstart_callback(self, fn: Callable[["FluxFuture"], None]) -> None:
        if getattr(self.proc, "started", -1.0) > 0:
            self._safeexec(fn)
            return

        try:
            self.proc.fut.add_event_callback("start", lambda _f, *args: self._safeexec(fn))
        except Exception:
            logger.debug("Failed to add Flux start callback", exc_info=True)

    def add_jobid_callback(self, fn: Callable[["FluxFuture"], None]) -> None:
        if self.proc.jobid != "unset":
            self._safeexec(fn)
            return

        try:
            self.proc.fut.add_jobid_callback(lambda _f: self._safeexec(fn))
        except Exception:
            logger.debug("Failed to add Flux jobid callback", exc_info=True)

    def add_callback(self, event: str, fn: Callable[["FluxFuture"], None]) -> None:
        if event == "done":
            self.add_done_callback(fn)
        elif event == "start":
            self.add_jobstart_callback(fn)
        elif event == "jobid":
            self.add_jobid_callback(fn)
        else:
            raise ValueError(f"Unknown callback event: {event!r}")

    def _safeexec(self, callback: Callable[["FluxFuture"], None]) -> None:
        try:
            callback(self)
        except Exception:
            logger.debug("FluxFuture callback failed", exc_info=True)

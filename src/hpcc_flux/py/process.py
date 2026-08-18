# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import time
from concurrent.futures import CancelledError
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable

import flux  # type: ignore[ty:unresolved-import, import-not-found]
import flux.job  # type: ignore[ty:unresolved-import, import-not-found]
from flux.job import FluxExecutorFuture  # type: ignore[ty:unresolved-import, import-not-found]

if TYPE_CHECKING:
    import flux.core.handle  # type: ignore[ty:unresolved-import, import-not-found]

import hpc_connect

logger = logging.getLogger("hpc_connect.flux.py.process")


class FluxProcess(hpc_connect.HPCProcess):
    JOB_TIMEOUT_CODE = 66

    def __init__(
        self, name: str, future: FluxExecutorFuture, handle: "flux.core.handle.Flux"
    ) -> None:
        self.handle = handle
        self.name = name
        self.fut: FluxExecutorFuture = future
        self._rc: int | None = None
        self.flux_jobid: flux.job.JobID | None = None
        self.jobid: str = "unset"
        self.completion_info: dict[str, Any] = {}

        self.fut.add_jobid_callback(self._callback("jobid", self._set_jobid))
        self.fut.add_done_callback(self._callback("returncode", self._set_returncode))
        self.fut.add_done_callback(self._callback("proc_info", self._set_proc_info))
        self.fut.add_event_callback("submit", self._callback("submit_time", self._set_submittime))
        self.fut.add_event_callback("start", self._callback("start_time", self._set_starttime))

    def _callback(self, name: str, fn: Callable[..., None]) -> Callable[..., None]:
        def wrapper(fut: FluxExecutorFuture, *args: Any) -> None:
            try:
                fn(fut, *args)
            except Exception as e:
                self.returncode = 1
                self._add_error(
                    kind=f"callback_{name}",
                    message=f"Unexpected error in Flux callback {name!r}",
                    exc=e,
                )
                logger.exception("Unexpected error in Flux callback %r for %s", name, self.name)

        return wrapper

    def _completion_info(self) -> dict[str, Any]:
        info = getattr(self, "completion_info", None)
        if isinstance(info, dict):
            return dict(info)
        return {}

    def _add_error(self, *, kind: str, message: str, exc: BaseException | None = None) -> None:
        info = self._completion_info()
        errors = list(info.get("hpc_connect_errors", []))

        entry: dict[str, Any] = {"kind": kind, "message": message, "jobid": self.jobid}

        if exc is not None:
            entry["exception_type"] = exc.__class__.__name__
            entry["repr"] = repr(exc)
            if isinstance(exc, OSError):
                entry["errno"] = exc.errno

        errors.append(entry)
        info["hpc_connect_errors"] = errors
        self.completion_info = info

    def _future_exception(self, fut: FluxExecutorFuture) -> BaseException | None:
        try:
            return fut.exception()
        except Exception as e:
            # If inspecting the future raises, keep that error rather than
            # losing it. Some Flux failures surface through this path.
            logger.debug("Could not inspect Flux future exception for %s", self.name, exc_info=True)
            return e

    def _set_returncode(self, fut: FluxExecutorFuture, *args: Any) -> None:
        exc = self._future_exception(fut)

        if exc is not None:
            self.returncode = 1
            self._add_error(kind="future_exception", message=str(exc), exc=exc)
            logger.error(
                "Flux job %s failed before a valid result was available (jobid=%s): %r",
                self.name,
                self.jobid,
                exc,
            )
            return

        try:
            info = flux.job.result(self.handle, fut.jobid())
            self.returncode = info.returncode
        except CancelledError:
            logger.warning("Flux job %s was cancelled", self.name)
            self.returncode = 1
            self._add_error(kind="cancelled", message="Flux job was cancelled")
        except Exception as e:
            self.returncode = 1
            self._add_error(
                kind="result_metadata", message="Failed while collecting Flux result metadata", exc=e
            )
            logger.exception(
                "Flux job %s failed while collecting result metadata (jobid=%s)",
                self.name,
                self.jobid,
            )

    def _set_jobid(self, fut: FluxExecutorFuture, *args: Any) -> None:
        try:
            self.flux_jobid = fut.jobid()
            self.jobid = str(self.flux_jobid)
            logger.debug("submitted job %s for %s", self.jobid, self.name)
        except CancelledError:
            self.returncode = 1
            self._add_error(
                kind="jobid_cancelled", message="Flux job was cancelled before jobid was assigned"
            )
        except Exception as e:
            self.returncode = 1
            self._add_error(kind="jobid", message="Failed to obtain Flux jobid", exc=e)
            logger.exception("Failed to obtain Flux jobid for %s", self.name)

    def _set_proc_info(self, fut: FluxExecutorFuture, *args: Any) -> None:
        exc = self._future_exception(fut)

        if exc is not None:
            self._add_error(kind="future_exception", message=str(exc), exc=exc)
            logger.error(
                "Flux job %s has future exception; skipping proc info lookup: %r", self.name, exc
            )
            return

        jobid = self.jobid

        if not jobid or jobid == "unset":
            self._add_error(
                kind="missing_jobid", message="Flux jobid was not assigned; submission likely failed"
            )
            logger.error(
                "Flux job %s did not receive a valid jobid; skipping proc info lookup", self.name
            )
            return

        try:
            job = flux.job.get_job(self.handle, flux.job.JobID(jobid))
        except Exception as e:
            self._add_error(kind="proc_info", message="Failed to query Flux job metadata", exc=e)
            logger.exception("Failed to query Flux job metadata for jobid=%s", jobid)
            return

        info = self._completion_info()
        info.update(dict(job))
        self.completion_info = info

    def _set_submittime(self, fut: FluxExecutorFuture, *args: Any) -> None:
        self.submitted = time.time()

    def _set_starttime(self, fut: FluxExecutorFuture, *args: Any) -> None:
        self.started = time.time()

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        return self.returncode

    def cancel(self) -> None:
        logger.warning("Canceling flux job %s", self.jobid)

        if self.flux_jobid is not None:
            try:
                flux.job.cancel(self.handle, self.flux_jobid)
            except OSError:
                logger.debug("Job %s is inactive, cannot cancel", self.jobid)
            except Exception:
                logger.exception("Failed to cancel job %s", self.jobid)

        self.returncode = 1

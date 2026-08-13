# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import atexit
import logging
import math
import os
import subprocess
from enum import IntEnum
from types import TracebackType
from typing import Literal
from typing import Sequence

logger = logging.getLogger("hpc_connect.flux.allocation")


class State(IntEnum):
    INACTIVE = 0
    ACTIVE = 1


class FluxAllocation:
    """Explicitly create and manage a background Flux allocation.

    Parameters
    ----------
    nodes : int
        Number of nodes to allocate.

    time_limit : float | str
        Allocation time limit.
        If a float or int is provided, it is interpreted as seconds.

    """

    def __init__(
        self, *, nodes: int = 1, time_limit: float | int = 3600.0, queue_timeout: float | int = 1200.0
    ) -> None:
        if nodes <= 0:
            raise ValueError(f"{nodes=} must be > 0")
        if isinstance(time_limit, (float, int)) and time_limit <= 0:
            raise ValueError(f"{time_limit=} must be > 0")

        self.nodes = nodes
        self.time_limit = time_limit
        self.queue_timeout = queue_timeout
        self.jobid: str | None = None
        self.uri: str | None = None
        self.state: State = State.INACTIVE

        self._parent_uri: str | None = None
        self._atexit_registered: bool = False

    def __enter__(self) -> "FluxAllocation":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> Literal[False]:
        self.close()
        return False

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def start(self, timeout: float | int | None = None) -> None:
        if timeout is None:
            timeout = self.queue_timeout
        if self.state != State.INACTIVE:
            raise RuntimeError("FluxAllocation already active")
        try:
            self.jobid = alloc(nodes=self.nodes, time_limit=self.time_limit, timeout=timeout)
            self.uri = uri(self.jobid)
            self._parent_uri = os.environ.get("FLUX_URI")
            os.environ["FLUX_URI"] = self.uri
            self.state = State.ACTIVE
            self._register_atexit()
            logger.info("Started Flux allocation %s with URI %s", self.jobid, self.uri)
        except Exception as e:
            logger.debug("Flux allocation startup failed", exc_info=True)
            jobid = self.jobid or "<unknown>"
            try:
                self.close()
            except Exception:
                logger.debug("Flux allocation cleanup after startup failure failed", exc_info=True)
            raise FluxAllocationStartupError(f"Failed to start Flux allocation {jobid}") from e
        if self.uri is None:
            raise FluxAllocationStartupError(f"Failed to obtain a Flux URI for job {self.jobid}")

    def close(self) -> None:
        if self.state != State.ACTIVE and self.jobid is None:
            return
        jobid = self.jobid
        self._unregister_atexit()
        try:
            self._restore_parent_uri()
            if jobid:
                logger.debug("Stopping Flux allocation job %s", jobid)
                kill(jobid)
        finally:
            self.jobid = None
            self.uri = None
            self.state = State.INACTIVE
            self._parent_uri = None

    def _restore_parent_uri(self) -> None:
        if self._parent_uri is not None:
            os.environ["FLUX_URI"] = self._parent_uri
        else:
            os.environ.pop("FLUX_URI", None)

    def _register_atexit(self) -> None:
        if not self._atexit_registered:
            atexit.register(self.close)
            self._atexit_registered = True

    def _unregister_atexit(self) -> None:
        if not self._atexit_registered:
            return
        try:
            atexit.unregister(self.close)
        except Exception:
            logger.debug("Failed to unregister atexit hook", exc_info=True)
        finally:
            self._atexit_registered = False


def alloc(
    nodes: int = 1,
    time_limit: float | int = 60.0,
    queue: str | None = None,
    job_name: str | None = None,
    timeout: float | None = None,
    extra_args: Sequence[str] | None = None,
) -> str:
    """
    Create a Flux allocation and return the Flux job ID.

    Parameters
    ----------
    nodes : int
        Number of nodes to allocate.

    time_limit : float | str
        Allocation time limit in seconds.

    queue : str, optional
        Flux queue/partition name, if required.

    job_name : str, optional
        Name for the allocation job.

    timeout : float, optional
        Maximum number of seconds to wait for the `flux alloc` command to
        return a job ID. This is a Python subprocess timeout, not the Flux
        allocation time limit.

    extra_args : sequence of str, optional
        Additional arguments to pass directly to `flux alloc`.

    Returns
    -------
    str
        The Flux job ID.

    Raises
    ------
    RuntimeError
        If the Flux allocation command fails, times out, or returns no job ID.
    """
    if nodes < 1:
        raise ValueError("nodes must be >= 1")
    # Flux prefers minutes
    minutes = math.ceil(float(time_limit) / 60.0)
    args = ["flux", "alloc", "--bg", f"-N{nodes}", f"-t{minutes}"]
    if queue:
        args.extend(["--queue", queue])
    if job_name:
        args.extend(["--job-name", job_name])
    if extra_args:
        args.extend(extra_args)
    try:
        cp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(
            "Timed out while waiting for Flux allocation command to return "
            f"a job ID after {timeout} seconds.\n"
            f"Command: {' '.join(args)}"
        ) from e

    if cp.returncode != 0:
        raise RuntimeError(
            "Flux allocation failed.\n"
            f"Command: {' '.join(args)}\n"
            f"Return code: {cp.returncode}\n"
            f"STDOUT: {cp.stdout}\n"
            f"STDERR: {cp.stderr}"
        )

    jobid = cp.stdout.strip()
    if not jobid:
        raise RuntimeError(
            "Flux allocation command completed but no job ID was returned.\n"
            f"Command: {' '.join(args)}\n"
            f"STDOUT: {cp.stdout}\n"
            f"STDERR: {cp.stderr}"
        )

    return jobid


def uri(jobid: str) -> str:
    """
    Get the Flux URI for a Flux job allocation.

    Parameters
    ----------
    jobid : str
        Flux job ID

    Returns
    -------
    str
        The Flux URI for the allocation.

    Raises
    ------
    ValueError
        If `jobid` is empty.

    RuntimeError
        If the URI lookup fails, times out, or returns no URI.
    """
    if not jobid:
        raise ValueError("jobid must be a non-empty string")
    args = ["flux", "uri", "--remote", jobid]
    try:
        cp = subprocess.run(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10.0, check=False
        )
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Timed out after 10 seconds while getting URI for job {jobid}") from e
    if cp.returncode != 0:
        raise RuntimeError(
            "Failed to get Flux URI.\n"
            f"Command: {' '.join(args)}\n"
            f"Return code: {cp.returncode}\n"
            f"STDOUT: {cp.stdout}\n"
            f"STDERR: {cp.stderr}"
        )
    uri = cp.stdout.strip()
    if not uri:
        raise RuntimeError(
            "Flux URI command completed but returned no URI.\n"
            f"Command: {' '.join(args)}\n"
            f"STDOUT: {cp.stdout}\n"
            f"STDERR: {cp.stderr}"
        )
    return uri


def kill(jobid: str) -> None:
    """
    Kill a Flux job.

    Parameters
    ----------
    jobid : str
        Flux job ID.

    """
    if not jobid:
        raise ValueError("jobid must be a non-empty string")
    args = ["flux", "job", "kill", jobid]
    try:
        subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
    except Exception:
        logger.debug(f"Failed to kill job {jobid}", exc_info=True)


class FluxAllocationStartupError(RuntimeError):
    pass

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import atexit
import logging
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

    Notes
    -----
    ``FluxAllocation`` stores only the allocation request shape and active
    allocation state. Runtime options such as time limit, queue timeout, and
    extra ``flux alloc`` arguments are supplied to ``open()``.
    """

    def __init__(self, nodes: int = 1) -> None:
        if nodes <= 0:
            raise ValueError(f"{nodes=} must be > 0")

        self.nodes = nodes

        self.jobid: str | None = None
        self.uri: str | None = None
        self.state: State = State.INACTIVE

        self._parent_uri: str | None = None
        self._atexit_registered: bool = False

    def __enter__(self) -> "FluxAllocation":
        if self.state != State.ACTIVE:
            raise RuntimeError("FluxAllocation must be opened before entering context")
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

    def open(self, args: Sequence[str], timeout: float | int = 1200.0) -> "FluxAllocation":
        if self.state != State.INACTIVE:
            raise RuntimeError("FluxAllocation already active")

        try:
            self.jobid = alloc(args, nodes=self.nodes, timeout=timeout)
            self.uri = uri(self.jobid)
            self._parent_uri = os.environ.get("FLUX_URI")
            os.environ["FLUX_URI"] = self.uri
            self.state = State.ACTIVE
            self._register_atexit()
            logger.debug("Started Flux allocation %s with URI %s", self.jobid, self.uri)

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

        return self

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


def alloc(args: Sequence[str], nodes: int = 1, timeout: float | None = None) -> str:
    """
    Create a Flux allocation and return the Flux job ID.

    Parameters
    ----------
    nodes : int
        Number of nodes to allocate.

    timeout : float, optional
        Maximum number of seconds to wait for the `flux alloc` command to
        return a job ID. This is a Python subprocess timeout, not the Flux
        allocation time limit.

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
    try:
        cp = subprocess.run(
            ["flux", "alloc", "--bg", f"-N{nodes}", *args],
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

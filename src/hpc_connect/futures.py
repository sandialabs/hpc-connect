# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import List
from typing import Literal
from typing import Optional

if TYPE_CHECKING:
    from .process import HPCProcess


CallbackEventT = Literal["start", "done", "jobid"]
valid_callback_events = {"start", "done", "jobid"}


class Future:
    def __init__(self, proc: "HPCProcess", polling_interval: float = 1.0):
        self.proc = proc
        self._polling_interval = polling_interval or 1.0
        self._callbacks: dict[CallbackEventT, List[Callable[["Future"], None]]] = defaultdict(list)
        self._done = threading.Event()
        self._cancelled = False
        self._lock = threading.Lock()

        # start polling in background
        threading.Thread(target=self._monitor, daemon=True).start()

    def _pop_callbacks(self, event: CallbackEventT) -> list[Callable[["Future"], None]]:
        with self._lock:
            return list(self._callbacks.pop(event, []))

    def _safeexec(self, callback: Callable[["Future"], None]) -> None:
        try:
            callback(self)
        except Exception:  # nosec B110
            pass

    def _exec_callbacks(self, event: CallbackEventT) -> None:
        callbacks = self._pop_callbacks(event)
        for cb in callbacks:
            self._safeexec(cb)

    def _monitor(self):
        while True:
            if self.proc.started > 0.0:
                self._exec_callbacks("start")
            if self.proc.jobid != "unset":
                self._exec_callbacks("jobid")
            if self.proc.poll() is not None:
                self._done.set()
                self._exec_callbacks("done")
                break
            if self._cancelled:
                break
            time.sleep(self._polling_interval)

    def done(self) -> bool:
        return self._done.is_set()

    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> bool:
        with self._lock:
            if self.done():
                return False
            self._cancelled = True
            try:
                self.proc.cancel()
            except Exception:  # nosec B110
                pass
            self._done.set()
            callbacks = list(self._callbacks.pop("done", []))
        for cb in callbacks:
            self._safeexec(cb)
        return True

    def result(self, timeout: Optional[float] = None) -> int:
        finished = self._done.wait(timeout=timeout)
        if not finished:
            raise TimeoutError(f"Job {self.proc.jobid} did not finish in time")
        rc = 1 if not isinstance(self.proc.returncode, int) else self.proc.returncode
        return rc

    def add_done_callback(self, fn: Callable[["Future"], None]):
        self.add_callback("done", fn)

    def add_jobstart_callback(self, fn: Callable[["Future"], None]):
        self.add_callback("start", fn)

    def add_jobid_callback(self, fn: Callable[["Future"], None]):
        self.add_callback("jobid", fn)

    def add_callback(self, event: CallbackEventT, fn: Callable[["Future"], None]):
        if event not in valid_callback_events:
            raise ValueError(f"Unknown callback event: {event!r}")
        call_now: bool = False
        with self._lock:
            if event == "done" and self.done():
                call_now = True
            elif event == "start" and self.proc.started > 0.0:
                call_now = True
            elif event == "jobid" and self.proc.jobid != "unset":
                call_now = True
            else:
                self._callbacks[event].append(fn)
        if call_now:
            self._safeexec(fn)

    @property
    def jobid(self) -> str:
        return self.proc.jobid

    @property
    def returncode(self) -> Optional[int]:
        return self.proc.returncode


def as_completed(
    futures: Iterable["Future"],
    timeout: float | None = None,
    polling_interval: float = 1.0,
    cancel_on_exception: bool = True,
) -> Generator["Future", None, None]:
    """
    Yield HPCFuture objects as they complete, similar to concurrent.futures.as_completed.

    Args:
        futures: Iterable of HPCFuture objects to monitor.
        timeout: Maximum number of seconds to wait for all futures. If None, wait indefinitely.
        polling_interval: Seconds between checks of each future's done() status.
        cancel_on_exception: If True, cancel all pending futures if an exception occurs during iteration.

    Yields:
        HPCFuture objects in the order they complete.

    Raises:
        TimeoutError: If the timeout expires before all futures are complete.
                       All pending futures are cancelled before raising.
        Exception: Propagates exceptions raised by iterating over futures. Pending futures are
                   cancelled first if cancel_on_exception is True.
    """
    pending = set(futures)
    start_time = time.monotonic()
    polling_interval = max(polling_interval, max([f._polling_interval for f in pending]))

    try:
        while pending:
            done_now = {fut for fut in pending if fut.done()}

            # Yield all newly done futures
            for fut in done_now:
                yield fut
                pending.remove(fut)

            # Check for timeout
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                if pending:
                    # Cancel all remaining pending HPC jobs
                    for fut in pending:
                        try:
                            fut.cancel()
                        except Exception as e:
                            print(f"Warning: failed to cancel future {fut}: {e}")
                    raise TimeoutError(
                        f"{len(pending)} futures did not complete within {timeout} seconds"
                    )
                break

            # Sleep briefly before polling again
            if pending:
                time.sleep(polling_interval)

    except Exception as e:
        # Optionally cancel pending futures on any exception
        if cancel_on_exception and pending:
            for fut in pending:
                try:
                    fut.cancel()
                except Exception as ce:
                    print(f"Warning: failed to cancel future {fut}: {ce}")
        raise  # propagate the original exception

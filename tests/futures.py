# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpc_connect.futures — Future and as_completed."""

import threading
import time

import pytest

from hpc_connect.futures import Future
from hpc_connect.futures import as_completed

# ---------------------------------------------------------------------------
# Minimal HPCProcess stand-in (no scheduler needed)
# ---------------------------------------------------------------------------


class _FakeProcess:
    """Synchronous fake that completes after ``delay`` seconds."""

    _submitted: float = -1.0
    _started: float = -1.0
    completion_info: dict | None = None

    def __init__(self, returncode: int = 0, delay: float = 0.0, jobid: str = "fake-1"):
        self._returncode: int | None = None
        self._jobid = jobid
        self._delay = delay
        self._cancelled = False

    @property
    def jobid(self) -> str:
        return self._jobid

    @jobid.setter
    def jobid(self, v: str) -> None:
        self._jobid = v

    @property
    def submitted(self) -> float:
        return self._submitted

    @submitted.setter
    def submitted(self, v: float) -> None:
        self._submitted = v

    @property
    def started(self) -> float:
        return self._started

    @started.setter
    def started(self, v: float) -> None:
        self._started = v

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @returncode.setter
    def returncode(self, v: int) -> None:
        self._returncode = v

    def poll(self) -> int | None:
        if self._cancelled:
            self._returncode = 1
            return 1
        if self._delay > 0:
            time.sleep(self._delay)
            self._delay = 0
        self._returncode = 0
        self.started = time.time()
        self.completion_info = {"state": "FINISHED"}
        return self._returncode

    def cancel(self) -> None:
        self._cancelled = True
        self._returncode = 1


# ---------------------------------------------------------------------------
# Future
# ---------------------------------------------------------------------------


def _make_future(delay: float = 0.0, returncode: int = 0, jobid: str = "fake-1") -> Future:
    proc = _FakeProcess(returncode=returncode, delay=delay, jobid=jobid)
    return Future(proc, polling_interval=0.01)


def test_future_completes_and_returns_zero():
    f = _make_future()
    assert f.result(timeout=2.0) == 0
    assert f.done()
    assert not f.cancelled()


def test_future_result_timeout_raises():
    # process that never finishes (large delay)
    proc = _FakeProcess(delay=9999)
    f = Future(proc, polling_interval=0.01)
    with pytest.raises(TimeoutError):
        f.result(timeout=0.05)


def test_future_done_callback_fires():
    called = threading.Event()
    f = _make_future()
    f.add_done_callback(lambda fut: called.set())
    assert called.wait(timeout=2.0), "done callback never fired"


def test_future_done_callback_fires_immediately_when_already_done():
    f = _make_future()
    f.result(timeout=2.0)
    called = threading.Event()
    f.add_done_callback(lambda fut: called.set())
    assert called.wait(timeout=0.5), "callback not called for already-done future"


def test_future_start_callback_fires():
    called = threading.Event()
    f = _make_future()
    f.add_jobstart_callback(lambda fut: called.set())
    assert called.wait(timeout=2.0), "start callback never fired"


def test_future_jobid_property():
    f = _make_future(jobid="job-xyz")
    assert f.jobid == "job-xyz"


def test_future_returncode_after_done():
    f = _make_future(returncode=0)
    f.result(timeout=2.0)
    assert f.returncode == 0


def test_future_cancel_before_done():
    # Use a slow process so we can cancel before it finishes
    proc = _FakeProcess(delay=9999)
    f = Future(proc, polling_interval=0.01)
    result = f.cancel()
    assert result is True
    assert f.cancelled()
    assert f.done()


def test_future_cancel_after_done_returns_false():
    f = _make_future()
    f.result(timeout=2.0)
    assert f.cancel() is False


def test_future_cancel_fires_done_callback():
    proc = _FakeProcess(delay=9999)
    f = Future(proc, polling_interval=0.01)
    called = threading.Event()
    f.add_done_callback(lambda fut: called.set())
    f.cancel()
    assert called.wait(timeout=1.0), "done callback not fired on cancel"


def test_future_add_callback_invalid_event():
    f = _make_future()
    with pytest.raises(ValueError, match="Unknown callback event"):
        f.add_callback("bogus", lambda fut: None)  # type: ignore[arg-type]


def test_future_proc_info_after_done():
    f = _make_future()
    f.result(timeout=2.0)
    info = f.proc_info()
    assert isinstance(info, dict)
    assert info.get("state") == "FINISHED"


def test_future_proc_info_no_completion_info():
    """When completion_info is None, proc_info() returns {}."""

    class _NoInfoProcess(_FakeProcess):
        def poll(self) -> int | None:
            self._returncode = 0
            self.started = time.time()
            # deliberately leave completion_info as None
            return self._returncode

    proc = _NoInfoProcess()
    f = Future(proc, polling_interval=0.01)
    f.result(timeout=2.0)
    assert f.proc_info() == {}


# ---------------------------------------------------------------------------
# _monitor resilience to poll() exceptions
# ---------------------------------------------------------------------------


class _FlakyProcess(_FakeProcess):
    """poll() raises ``fail_times`` times, then completes normally."""

    def __init__(self, fail_times: int, returncode: int = 0, jobid: str = "flaky-1"):
        super().__init__(returncode=returncode, jobid=jobid)
        self._fail_times = fail_times
        self._target_rc = returncode

    def poll(self) -> int | None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise RuntimeError("transient scheduler query failure")
        self._returncode = self._target_rc
        self.started = time.time()
        self.completion_info = {"state": "FINISHED"}
        return self._returncode


class _AlwaysFailingProcess(_FakeProcess):
    """poll() always raises — emulates a permanently lost scheduler record."""

    def poll(self) -> int | None:
        raise RuntimeError("scheduler record gone")


def test_future_survives_transient_poll_errors():
    """A few consecutive poll() failures must not strand the future."""
    proc = _FlakyProcess(fail_times=3, returncode=0)
    f = Future(proc, polling_interval=0.01, max_consecutive_poll_errors=10)
    assert f.result(timeout=2.0) == 0
    assert f.done()
    assert not f.poll_failed()


def test_future_consecutive_error_counter_resets():
    """Interleaved failures below the threshold still allow completion."""
    # 5 failures then success, with a budget of 6 — must complete cleanly.
    proc = _FlakyProcess(fail_times=5, returncode=0)
    f = Future(proc, polling_interval=0.01, max_consecutive_poll_errors=6)
    assert f.result(timeout=2.0) == 0
    assert not f.poll_failed()


def test_future_gives_up_after_max_consecutive_poll_errors():
    """A permanently failing poll() resolves the future instead of hanging."""
    proc = _AlwaysFailingProcess()
    f = Future(proc, polling_interval=0.01, max_consecutive_poll_errors=3)
    # Must not raise TimeoutError — the monitor gives up and marks it done.
    rc = f.result(timeout=2.0)
    assert f.done()
    assert f.poll_failed()
    # Lost job is treated as a failure (no returncode was ever reported).
    assert rc == 1


def test_future_giveup_fires_done_callback():
    proc = _AlwaysFailingProcess()
    f = Future(proc, polling_interval=0.01, max_consecutive_poll_errors=2)
    called = threading.Event()
    f.add_done_callback(lambda fut: called.set())
    assert called.wait(timeout=2.0), "done callback not fired on poll give-up"
    assert f.poll_failed()


# ---------------------------------------------------------------------------
# as_completed
# ---------------------------------------------------------------------------
def test_as_completed_yields_all():
    futures = [_make_future(jobid=f"j{i}") for i in range(3)]
    done = list(as_completed(futures, polling_interval=0.01))
    assert len(done) == 3
    assert set(f.jobid for f in done) == {"j0", "j1", "j2"}


def test_as_completed_timeout_cancels_pending():
    # One fast, one slow
    fast = _make_future(delay=0.0, jobid="fast")
    slow_proc = _FakeProcess(delay=9999, jobid="slow")
    slow = Future(slow_proc, polling_interval=0.01)

    results = []
    with pytest.raises(TimeoutError):
        for f in as_completed([fast, slow], timeout=0.3, polling_interval=0.01):
            results.append(f.jobid)

    assert "fast" in results
    assert slow.cancelled()


def test_as_completed_empty():
    assert list(as_completed([], polling_interval=0.01)) == []

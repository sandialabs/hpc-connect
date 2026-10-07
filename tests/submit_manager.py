# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import Any

from hpc_connect.futures import Future
from hpc_connect.jobspec import JobSpec
from hpc_connect.process import HPCProcess
from hpc_connect.submit import HPCSubmissionManager
from hpc_connect.submit import SubmissionAdapter


class FakeProcess(HPCProcess):
    def __init__(self, returncode: int | None = 0) -> None:
        self._returncode = returncode

    @property
    def returncode(self) -> int | None:
        return self._returncode

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._returncode = arg

    def poll(self) -> int | None:
        return self._returncode

    def cancel(self) -> None:
        self._returncode = 1


class FakeAdapter(SubmissionAdapter):
    def __init__(self, *, config: dict[str, Any], proc: FakeProcess) -> None:
        super().__init__(config=config)
        self.proc = proc
        self.calls: list[tuple[JobSpec, bool]] = []

    def submit(self, spec: JobSpec, exclusive: bool = True) -> FakeProcess:
        self.calls.append((spec, exclusive))
        return self.proc


def test_submission_manager_submit_returns_future():
    proc = FakeProcess(returncode=0)
    adapter = FakeAdapter(config={"polling_interval": 2.5}, proc=proc)
    manager = HPCSubmissionManager(adapter=adapter)
    spec = JobSpec(name="j", commands=["true"])

    future = manager.submit(spec, exclusive=False)

    assert isinstance(future, Future)
    assert adapter.calls == [(spec, False)]
    assert future.result(timeout=0.1) == 0
    assert future._polling_interval == 2.5


def test_submission_manager_popen_returns_process_directly():
    proc = FakeProcess(returncode=0)
    adapter = FakeAdapter(config={"polling_interval": 1.0}, proc=proc)
    manager = HPCSubmissionManager(adapter=adapter)
    spec = JobSpec(name="j", commands=["true"])

    returned = manager.popen(spec)

    assert returned is proc
    assert adapter.calls == [(spec, True)]


def test_submission_adapter_default_polling_interval_is_one_second():
    adapter = FakeAdapter(config={}, proc=FakeProcess())
    assert adapter.polling_interval() == 1.0

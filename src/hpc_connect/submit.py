# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import Protocol

from .futures import Future
from .futures import FutureProtocol
from .jobspec import JobSpec
from .process import HPCProcess


class SubmissionFailedError(Exception):
    pass


class Adapter(Protocol):
    def submit(self, spec: JobSpec, exclusive: bool = False) -> HPCProcess: ...
    def polling_interval(self) -> float: ...


class SubmissionManagerProtocol(Protocol):
    def submit(self, spec: JobSpec, exclusive: bool = True) -> FutureProtocol: ...
    def popen(self, spec: JobSpec, exclusive: bool = True) -> HPCProcess: ...


class HPCSubmissionManager:
    def __init__(self, *, adapter: Adapter) -> None:
        self.adapter = adapter

    def submit(self, spec: JobSpec, exclusive: bool = True) -> Future:
        proc = self.adapter.submit(spec, exclusive=exclusive)
        return Future(proc, polling_interval=self.adapter.polling_interval() or 1.0)

    def popen(self, spec: JobSpec, exclusive: bool = True) -> HPCProcess:
        return self.adapter.submit(spec, exclusive=exclusive)

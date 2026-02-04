# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import Protocol

from .futures import Future
from .jobspec import JobSpec
from .process import HPCProcess


class Adapter(Protocol):
    def submit(self, spec: JobSpec, exclusive: bool = False) -> HPCProcess: ...
    def polling_interval(self) -> float: ...


class HPCSubmissionManager:
    def __init__(self, *, adapter: Adapter) -> None:
        self.adapter = adapter

    def submit(self, spec: JobSpec, exclusive: bool = True) -> Future:
        proc = self.adapter.submit(spec, exclusive=exclusive)
        return Future(proc, polling_interval=self.adapter.polling_interval() or 1.0)

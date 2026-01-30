# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib.resources
import logging
import os
import shutil
from typing import Any

import hpc_connect
from hpc_connect.util import time_in_seconds

from .process import RemoteSubprocess

logger = logging.getLogger("hpc_connect.remote.backend")


class RemoteBackend(hpc_connect.Backend):
    name = "remote_subprocess"

    def __init__(self) -> None:
        ssh = shutil.which("ssh")
        if ssh is None:
            raise ValueError("ssh not found on PATH")

    @property
    def polling_frequency(self) -> float:
        s = os.getenv("HPCC_POLLING_FREQUENCY") or 0.5
        return time_in_seconds(s)

    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
        **kwargs: Any,
    ) -> hpc_connect.HPCProcess:
        host = kwargs.get("host")
        if host is None:
            raise ValueError("missing required kwarg 'host'")
        cpus = cpus or kwargs.get("tasks")  # backward compatible
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
        )
        assert script is not None
        return RemoteSubprocess(host, script, output=output, error=error)

    @property
    def submission_template(self) -> str:
        return str(importlib.resources.files("hpcc_remote").joinpath("templates/submit.sh.in"))

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
import shlex
import shutil

import hpc_connect
from hpc_connect.discover import default_resource_set
from hpc_connect.util.time import time_in_seconds

from .process import Subprocess

logger = logging.getLogger("hpc_connect.subprocess.backend")


class LocalBackend(hpc_connect.Backend):
    name = "shell"

    def __init__(self) -> None:
        self._resource_specs = default_resource_set()

    @property
    def resource_specs(self) -> list[dict]:
        return self._resource_specs

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        return hpc_connect.HPCSubmissionManager(adapter=SubprocessAdapter())


class SubprocessAdapter:
    def __init__(self):
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    def poll_interval(self) -> float:
        s = os.getenv("HPCC_POLL_INTERVAL") or 0.5
        return time_in_seconds(s)

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            for var, val in spec.env.items():
                if val is None:
                    fh.write(f"unset {var}\n")
                else:
                    fh.write(f'export {var}="{val}"\n')
            for command in spec.commands:
                fh.write(f"{command}\n")
        os.chmod(script, 0o755)
        return spec.with_updates(commands=[f"{sh} {script}"])

    def submit(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> hpc_connect.HPCProcess:
        s = self.prepare(spec)
        return Subprocess(shlex.split(s.commands[0]), output=spec.output, error=spec.error)

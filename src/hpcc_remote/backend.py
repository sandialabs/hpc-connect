# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
import shutil

import hpc_connect
from hpc_connect.util import time_in_seconds

from .process import RemoteSubprocess

logger = logging.getLogger("hpc_connect.remote.backend")


class RemoteBackend(hpc_connect.Backend):
    name = "remote_subprocess"

    def __init__(self, config: hpc_connect.Config | None = None) -> None:
        super().__init__(config=config)
        ssh = shutil.which("ssh")
        if ssh is None:
            raise ValueError("ssh not found on PATH")

    @property
    def resource_specs(self) -> list[dict]:
        raise NotImplementedError

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        return hpc_connect.HPCSubmissionManager(adapter=RemoteAdapter())

    def launcher(self) -> hpc_connect.HPCLauncher:
        raise NotImplementedError


class RemoteAdapter:
    def poll_interval(self) -> float:
        s = os.getenv("HPCC_POLLING_FREQUENCY") or 0.5
        return time_in_seconds(s)

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            for arg in spec.submit_args:
                fh.write(f"#BASH {arg}\n")
            for var, val in spec.env.items():
                if val is None:
                    fh.write(f"unset {var}\n")
                else:
                    fh.write(f'export {var}="{val}"\n')
            for command in spec.commands:
                fh.write(f"{command}\n")
        os.chmod(script, 0o755)
        return spec.with_updates(commands=[script])

    def submit(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> hpc_connect.HPCProcess:
        s = self.prepare(spec)
        host = spec.extensions.get("remote_subprocess", {}).get("host")
        if host is None:
            raise ValueError("missing required kwarg 'host'")
        return RemoteSubprocess(host, s.commands[0], output=spec.output, error=spec.error)

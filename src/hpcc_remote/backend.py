# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shutil

import hpc_connect
from hpc_connect.config import SubmitConfig
from hpc_connect.util import set_executable

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
        config = self.config.submit.resolve("remote_subprocess")
        return hpc_connect.HPCSubmissionManager(adapter=RemoteAdapter(config=config))

    def launcher(self) -> hpc_connect.HPCLauncher:
        raise NotImplementedError


class RemoteAdapter:
    def __init__(self, config: SubmitConfig) -> None:
        self.config = config

    def polling_interval(self) -> float:
        return self.config.polling_interval or 0.5

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
        set_executable(script)
        return spec.with_updates(commands=[script])

    def submit(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> hpc_connect.HPCProcess:
        s = self.prepare(spec)
        host = spec.extensions.get("remote_subprocess", {}).get("host")
        if host is None:
            raise ValueError("missing required kwarg 'host'")
        return RemoteSubprocess(host, s.commands[0], output=spec.output, error=spec.error)

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import fnmatch
import json
import logging
import os
import shlex
import shutil
import subprocess
import time
import weakref
from typing import Any
from typing import TextIO

import psutil

from .backend import Backend
from .config import Config
from .config import SubmitConfig
from .hookspec import hookimpl
from .jobspec import JobSpec
from .launch import HPCLauncher
from .mpi import MPIExecAdapter
from .process import HPCProcess
from .submit import HPCSubmissionManager

logger = logging.getLogger("hpc_connect.subprocess.backend")


class LocalBackend(Backend):
    name = "local"

    def __init__(self, config: Config | None = None) -> None:
        super().__init__(config=config)
        self._resource_specs = self.discover()

    @property
    def resource_specs(self) -> list[dict]:
        return self._resource_specs

    def submission_manager(self) -> HPCSubmissionManager:
        config = self.config.submit.resolve("local")
        return HPCSubmissionManager(adapter=SubprocessAdapter(config=config))

    def launcher(self) -> HPCLauncher:
        return HPCLauncher(
            adapter=MPIExecAdapter(backend=self, config=self.config.launch.resolve("mpiexec"))
        )

    def discover(self) -> list[dict[str, Any]]:
        if file := os.getenv("HPC_CONNECT_HOSTFILE"):
            with open(file) as fh:
                data = json.load(fh)
            host: str = os.getenv("HPC_CONNECT_HOSTNAME") or os.uname().nodename
            for pattern, rspec in data.items():
                if fnmatch.fnmatch(host, pattern):
                    return rspec
        local_resource = {"type": "cpu", "count": psutil.cpu_count()}
        socket_resource = {"type": "socket", "count": 1, "resources": [local_resource]}
        return [{"type": "node", "count": 1, "resources": [socket_resource]}]


class SubprocessAdapter:
    def __init__(self, config: SubmitConfig):
        self.config = config
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    def polling_interval(self) -> float:
        return self.config.polling_interval or 1.0

    def prepare(self, spec: JobSpec) -> JobSpec:
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
        return spec.with_updates(commands=[f"{sh} {script}"])

    def submit(self, spec: JobSpec, exclusive: bool = True) -> "Subprocess":
        s = self.prepare(spec)
        return Subprocess(shlex.split(s.commands[0]), output=spec.output, error=spec.error)


class Subprocess(HPCProcess):
    def __init__(
        self, args: list[str], output: str | None, error: str | None, emit_interval: float = 300.0
    ) -> None:
        stdout = streamify(output)
        if stdout is not None:
            weakref.finalize(stdout, stdout.close)
        stderr: TextIO | int | None
        if error is None:
            stderr = None
        elif error == output:
            stderr = subprocess.STDOUT
        else:
            stderr = streamify(error)
        if hasattr(stderr, "write"):
            weakref.finalize(stderr, stderr.close)  # type: ignore
        self.submitted = self.started = time.time()
        self.proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
        self.jobid = str(self.proc.pid)
        self.last_debug_emit: float = -1
        self.emit_interval: float = emit_interval

    @property
    def returncode(self) -> int | None:
        return self.proc.returncode

    @returncode.setter
    def returncode(self, arg: int) -> None:
        raise NotImplementedError

    def poll(self) -> int | None:
        rc = self.proc.poll()
        now = time.monotonic()
        if now - self.last_debug_emit >= self.emit_interval:
            logger.debug(f"Polling running job with pid {self.proc.pid}")
            self.last_debug_emit = now
        return rc

    def cancel(self) -> None:
        """Kill a process tree (including grandchildren)"""
        logger.warning(f"cancelling shell batch with pid {self.proc.pid}")
        try:
            parent = psutil.Process(self.proc.pid)
        except psutil.NoSuchProcess:
            return
        children = parent.children(recursive=True)
        children.append(parent)
        for p in children:
            try:
                p.terminate()
            except Exception:  # nosec B110
                pass
        _, alive = psutil.wait_procs(children)
        for p in alive:
            try:
                p.kill()
            except Exception:  # nosec B110
                pass


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


@hookimpl
def hpc_connect_backend(config: Config) -> "LocalBackend | None":
    if config.backend in ("local", "shell", "subprocess"):
        return LocalBackend(config=config)
    return None

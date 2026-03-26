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
from typing import Type

import psutil

from .backend import Backend
from .hookspec import hookimpl
from .jobspec import JobSpec
from .launch import HPCLauncher
from .mpi import MPIExecAdapter
from .process import HPCProcess
from .submit import HPCSubmissionManager
from .util import set_executable

logger = logging.getLogger("hpc_connect.subprocess.backend")


class LocalBackend(Backend):
    name = "local"

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self._resource_specs: list[dict[str, Any]] | None = None
        super().__init__(cfg=cfg)

    @classmethod
    def matches(cls, arg: str) -> bool:
        return arg in (cls.name, "shell")

    @property
    def resource_specs(self) -> list[dict]:
        if self._resource_specs is None:
            self._resource_specs = self.discover()
        assert self._resource_specs is not None
        return self._resource_specs

    @property
    def valid_launchers(self) -> set[str]:
        return {"mpi"}

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return {
            "config": {},
            "type": cls.name,
            "launch": {
                "type": "mpi",
                "exec": "mpiexec",
                "numproc_flag": "-n",
                "default_options": [],
                "pre_options": [],
                "mpmd": {
                    "global_options": [],
                    "local_options": [],
                },
            },
            "submit": {
                "default_options": [],
                "polling_interval": -1.0,
            },
        }

    def submission_manager(self) -> HPCSubmissionManager:
        return HPCSubmissionManager(adapter=SubprocessAdapter(config=self.config["submit"]))

    def launcher(self) -> HPCLauncher:
        return HPCLauncher(adapter=MPIExecAdapter(backend=self, config=self.config["launch"]))

    def discover(self) -> list[dict[str, Any]]:
        if file := os.getenv("HPC_CONNECT_HOSTFILE"):
            with open(file) as fh:
                data = json.load(fh)
            host: str = os.getenv("HPC_CONNECT_HOSTNAME") or os.uname().nodename
            for pattern, rspec in data.items():
                if fnmatch.fnmatch(host, pattern):
                    return rspec
        cfg: dict[str, Any] = self.config["config"]
        cpu_count: int = cfg.get("cores_per_socket") or psutil.cpu_count() or 1
        sockets_per_node: int = cfg.get("sockets_per_node") or 1
        node_count: int = cfg.get("nnode") or 1

        local_resource = {"type": "cpu", "count": cpu_count}
        socket_resource = {"type": "socket", "count": sockets_per_node, "resources": [local_resource]}
        return [{"type": "node", "count": node_count, "resources": [socket_resource]}]


class SubprocessAdapter:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    def polling_interval(self) -> float:
        if self.config["polling_interval"] > 0:
            return self.config["polling_interval"]
        return 1.0

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
        set_executable(script)
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
def hpc_connect_backend() -> Type[LocalBackend]:
    return LocalBackend

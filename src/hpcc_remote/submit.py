# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib.resources
import logging
import os
import shutil
import subprocess
import weakref
from typing import Any
from typing import TextIO

import psutil

from hpc_connect.config import Config
from hpc_connect.submit import HPCProcess
from hpc_connect.submit import HPCSubmissionManager
from hpc_connect.util import time_in_seconds

logger = logging.getLogger(__name__)


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


class RemoteSubprocess(HPCProcess):
    def __init__(
        self,
        host: str,
        script: str,
        output: str | None = None,
        error: str | None = None,
    ) -> None:
        ssh = shutil.which("ssh")
        if ssh is None:
            raise RuntimeError("ssh not found on PATH")
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
        self.proc = subprocess.Popen([ssh, host, script], stdout=stdout, stderr=stderr)

    @property
    def returncode(self) -> int | None:
        return self.proc.returncode

    @returncode.setter
    def returncode(self, arg: int) -> None:
        raise NotImplementedError

    def poll(self) -> int | None:
        return self.proc.poll()

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


class RemoteSubprocessSubmissionManager(HPCSubmissionManager):
    name = "remote_subprocess"

    def __init__(self, config: Config | None = None):
        super().__init__(config=config)
        ssh = shutil.which("ssh")
        if ssh is None:
            raise ValueError("ssh not found on PATH")

    @staticmethod
    def matches(name) -> bool:
        return name in ("remote_subprocess", "ssh")

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
    ) -> HPCProcess:
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

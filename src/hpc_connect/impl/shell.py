# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib
import logging
import os
import shutil
import subprocess
import weakref
from typing import Any
from typing import TextIO

import psutil

from ..hookspec import hookimpl
from ..types import HPCBackend
from ..types import HPCProcess
from ..util import time_in_seconds

logger = logging.getLogger("hpc_connect")


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


class ShellProcess(HPCProcess):
    def __init__(
        self,
        script: str,
        output: str | None = None,
        error: str | None = None,
    ) -> None:
        sh = shutil.which("sh")
        if sh is None:
            raise RuntimeError("sh not found on PATH")
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
        self.proc = subprocess.Popen([sh, script], stdout=stdout, stderr=stderr)

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
            except Exception:
                pass
        _, alive = psutil.wait_procs(children)
        for p in alive:
            try:
                p.kill()
            except Exception:
                pass


class ShellBackend(HPCBackend):
    name = "shell"

    def __init__(self):
        super().__init__()
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    @staticmethod
    def matches(name) -> bool:
        return name in ("shell", "none")

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
        return ShellProcess(script, output=output, error=error)

    @property
    def submission_template(self) -> str:
        return str(importlib.resources.files("hpc_connect").joinpath("templates/shell.sh.in"))


@hookimpl
def hpc_connect_backend():
    return ShellBackend

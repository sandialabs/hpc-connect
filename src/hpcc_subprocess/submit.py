# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib.resources
import os
import shutil
import subprocess
import time
import weakref
from typing import Any
from typing import TextIO

import psutil

import hpc_connect
from hpc_connect.util import time_in_seconds

logger = hpc_connect.get_logger(__name__)


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


class Subprocess(hpc_connect.HPCProcess):
    def __init__(
        self,
        script: str,
        output: str | None = None,
        error: str | None = None,
        emit_interval: float = 300.0,
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


class SubprocessSubmissionManager(hpc_connect.HPCSubmissionManager):
    name = "shell"

    def __init__(self, config: hpc_connect.Config | None = None):
        super().__init__(config=config)
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    @staticmethod
    def matches(name) -> bool:
        return name in ("subprocess", "shell", "none")

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
        return Subprocess(script, output=output, error=error)

    @property
    def submission_template(self) -> str:
        return str(importlib.resources.files("hpcc_subprocess").joinpath("templates/submit.sh.in"))

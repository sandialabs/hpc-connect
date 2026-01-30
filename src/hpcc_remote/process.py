# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
import shutil
import subprocess
import weakref
from typing import TextIO

import psutil

import hpc_connect

logger = logging.getLogger("hpc_connect.remote.process")


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


class RemoteSubprocess(hpc_connect.HPCProcess):
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

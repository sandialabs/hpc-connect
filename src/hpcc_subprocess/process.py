# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import logging
import os
import subprocess
import time
import weakref
from typing import TextIO

import psutil

import hpc_connect

logger = logging.getLogger("hpc_connect.subprocess.process")


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.dirname(arg), exist_ok=True)
    return open(arg, mode="w")


class Subprocess(hpc_connect.HPCProcess):
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
        self.proc = subprocess.Popen(args, stdout=stdout, stderr=stderr)
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

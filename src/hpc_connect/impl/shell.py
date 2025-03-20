import importlib
import os
import shutil
import subprocess
import weakref
from typing import TextIO

import psutil

from ..hookspec import hookimpl
from ..types import HPCBackend
from ..types import HPCProcess


def streamify(arg: str | None) -> TextIO | None:
    if arg is None:
        return None
    os.makedirs(os.path.basename(arg), exist_ok=True)
    return open(arg, mode="w")


class ShellProcess(subprocess.Popen, HPCProcess):
    def __init__(
        self,
        script: str,
        output: str | None = None,
        error: str | None = None,
        env: dict[str, str] | None = None,
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
        super().__init__([sh, script], stdout=stdout, stderr=stderr, env=env)

    def cancel(self) -> None:
        """Kill a process tree (including grandchildren)"""
        try:
            parent = psutil.Process(self.pid)
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


class ShellScheduler(HPCBackend):
    name = "shell"

    def __init__(self):
        sh = shutil.which("sh")
        if sh is None:
            raise ValueError("sh not found on PATH")

    @staticmethod
    def matches(name) -> bool:
        return name == "shell"

    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        batch_options: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
    ) -> HPCProcess:
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=qtime,
            batch_options=batch_options,
            variables=variables,
            output=output,
            error=error,
            tasks=tasks,
            cpus_per_task=cpus_per_task,
            gpus_per_task=gpus_per_task,
            tasks_per_node=tasks_per_node,
            nodes=nodes,
        )
        return ShellProcess(script, output=output, error=error)

    @property
    def submission_template(self) -> str:
        return str(importlib.resources.files("hpc_connect").joinpath("templates/shell.sh.in"))


@hookimpl
def hpc_connect_backend():
    return ShellScheduler

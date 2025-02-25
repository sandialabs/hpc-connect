import getpass
import io
import os
import shutil
import subprocess
from datetime import datetime
from typing import TextIO

from .hookspec import hookimpl
from .job import Job
from .submit import HPCProcess
from .submit import HPCScheduler
from .util import hhmmss
from .util import set_executable


class ShellProcess(HPCProcess):
    def __init__(self, job: Job) -> None:
        super().__init__(job=job)
        sh = shutil.which("sh")
        if sh is None:
            raise RuntimeError("sh not found on PATH")
        if job.output:
            self.stdout = open(job.output, "w")
        if job.error:
            if isinstance(self.stdout, io.IOBase) and self.stdout.name == job.error:
                self.stderr = subprocess.STDOUT
            else:
                self.stderr = open(job.error, "w")

        args = [sh, job.script]
        self.proc: subprocess.Popen = subprocess.Popen(
            args, stdout=self.stdout, stderr=self.stderr
        )

    def cancel(self, returncode: int) -> None:
        self.proc.terminate()
        self.returncode = returncode

    def poll(self) -> int | None:
        self.returncode = self.proc.poll()
        return self.returncode


class ShellScheduler(HPCScheduler):
    """Default 'scheduler' submits jobs to the shell"""

    name = "shell"
    shell = "/bin/sh"

    @staticmethod
    def matches(name: str | None) -> bool:
        if name is None:
            return False
        return name.lower() in ("shell", "subshell", "none")

    def write_submission_script(self, job: Job, file: TextIO) -> None:
        file.write(f"#!{self.shell}\n")
        args = list(self.default_args)
        for arg in args:
            file.write(f"# BASH: {arg}\n")
        file.write(f"# user: {getpass.getuser()}\n")
        file.write(f"# date: {datetime.now().strftime('%c')}\n")
        file.write(f"# output: {job.output}\n")
        file.write(f"# error: {job.error}\n")
        file.write(f"# qtime: {job.qtime}\n")
        file.write(f"# approximate runtime: {hhmmss(job.qtime)}\n")
        if job.variables is not None:
            for var, val in job.variables.items():
                if val is None:
                    file.write(f"unset {var}\n")
                else:
                    file.write(f"export {var}={val}\n")
        for command in job.commands:
            file.write(f"{command}\n")

    def submit(self, job: Job) -> HPCProcess:
        os.makedirs(os.path.dirname(job.script), exist_ok=True)
        with open(job.script, "w") as fh:
            self.write_submission_script(job, fh)
        set_executable(job.script)
        return ShellProcess(job)


@hookimpl
def hpc_connect_scheduler():
    return ShellScheduler

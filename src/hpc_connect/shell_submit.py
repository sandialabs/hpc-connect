import getpass
import io
import os
import re
import shutil
import subprocess
from datetime import datetime
from typing import TextIO

from .job import Job
from .submit import HPCProcess
from .submit import HPCScheduler
from .util import hhmmss
from .util import set_executable


class ShellProcess(HPCProcess):
    def __init__(self, script: str, *, job_name: str | None = None) -> None:
        super().__init__(script, job_name=job_name)
        sh = shutil.which("sh")
        if sh is None:
            raise RuntimeError("sh not found on PATH")
        text = open(script).read()
        if match := re.search(r"^# output: (?!None).*$", text, re.MULTILINE):
            f = match.group()[9:].strip()
            self.stdout = open(f, "w")
        if match := re.search(r"^# error: (?!None).*$", text, re.MULTILINE):
            f = match.group()[8:].strip()
            if isinstance(self.stdout, io.IOBase) and self.stdout.name == f:
                self.stderr = subprocess.STDOUT
            else:
                self.stderr = open(f, "w")
        args = [sh, script]
        self.proc: subprocess.Popen = subprocess.Popen(
            args, stdout=self.stdout, stderr=self.stderr
        )

    def cancel(self) -> None:
        return self.proc.terminate()

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
        return ShellProcess(job.script, job_name=job.name)

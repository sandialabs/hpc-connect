import io
import os
import shutil
import subprocess

from .hookspec import hookimpl
from .job import Job
from .submit import HPCProcess
from .submit import HPCScheduler
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

    @property
    def submission_template(self) -> str:
        import importlib.resources

        return str(importlib.resources.files("hpc_connect").joinpath("shell.sh.in"))

    def submit(self, job: Job) -> HPCProcess:
        os.makedirs(os.path.dirname(job.script), exist_ok=True)
        with open(job.script, "w") as fh:
            self.write_submission_script(job, fh)
        set_executable(job.script)
        return ShellProcess(job)


@hookimpl
def hpc_connect_scheduler():
    return ShellScheduler

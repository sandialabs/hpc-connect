import getpass
import io
import re
import shutil
import subprocess
from datetime import datetime
from typing import Optional
from typing import TextIO

from .submit import HPCProcess
from .submit import HPCScheduler
from .util import hhmmss


class ShellProcess(HPCProcess):
    def __init__(self, script: str, *, job_name: Optional[str] = None) -> None:
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

    def poll(self) -> Optional[int]:
        self.returncode = self.proc.poll()
        return self.returncode


class ShellScheduler(HPCScheduler):
    """Default 'scheduler' submits jobs to the shell"""

    name = "shell"
    shell = "/bin/sh"

    @staticmethod
    def matches(name: Optional[str]) -> bool:
        if name is None:
            return False
        return name.lower() in ("shell", "subshell", "none")

    def write_submission_script(
        self,
        script: list[str],
        file: TextIO,
        *,
        tasks: int,
        nodes: Optional[int] = None,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
        qtime: Optional[float] = None,
        variables: Optional[dict[str, Optional[str]]] = None,
    ) -> None:
        file.write(f"#!{self.shell}\n")
        file.write(f"# user: {getpass.getuser()}\n")
        file.write(f"# date: {datetime.now().strftime('%c')}\n")
        file.write(f"# output: {output}\n")
        file.write(f"# error: {error}\n")
        file.write(f"# qtime: {qtime}\n")
        file.write(f"# approximate runtime: {hhmmss(qtime)}\n")
        if variables is not None:
            for var, val in variables.items():
                if val is None:
                    file.write(f"unset {var}\n")
                else:
                    file.write(f"export {var}={val}\n")
        for line in script:
            file.write(f"{line}\n")

    def submit(self, script: str, job_name: Optional[str] = None) -> HPCProcess:
        return ShellProcess(script, job_name=job_name)

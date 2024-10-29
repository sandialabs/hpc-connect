import getpass
import subprocess
import sys
from datetime import datetime
from typing import Optional
from typing import TextIO

from .submit import HPCScheduler
from .util import hhmmss


class ShellScheduler(HPCScheduler):
    """Default 'scheduler' submits jobs to the shell"""

    name = "shell"
    shell = "/bin/sh"
    command_name = "sh"

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
        file.write(f"# approximate runtime: {hhmmss(qtime)}\n")
        if variables is not None:
            for var, val in variables.items():
                if val is None:
                    file.write(f"unset {var}\n")
                else:
                    file.write(f"export {var}={val}\n")
        for line in script:
            file.write(f"{line}\n")

    def submit_and_wait(
        self,
        script: str,
        job_name: Optional[str] = None,
        output: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        fo: TextIO = sys.stdout
        fe: TextIO = sys.stderr
        own_fo = own_fe = False
        if output is not None:
            fo = open(output, "w")
            own_fo = True
        if error is not None:
            if error == output:
                fe = fo
            else:
                fe = open(error, "w")
                own_fe = True
        try:
            args = [self.exe, script]
            proc = subprocess.Popen(args, stdout=fo, stderr=fe)
            proc.wait()
        except subprocess.CalledProcessError:
            pass
        finally:
            if own_fo:
                fo.close()
            if own_fe:
                fe.close()
        return

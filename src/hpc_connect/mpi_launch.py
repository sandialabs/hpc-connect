import os
import shutil
import subprocess
from typing import Any

from .launch import HPCLauncher


class MPILauncher(HPCLauncher):
    name = "mpi"

    def __init__(self) -> None:
        if exe := shutil.which("mpiexec"):
            self.exe = exe
        elif exe := shutil.which("mpirun"):
            self.exe = exe
        else:
            raise ValueError("mpiexec not found on PATH")

    @staticmethod
    def matches(name: str) -> bool:
        if name == MPILauncher.name:
            return True
        elif os.path.basename(name) in ("mpirun", "mpiexec"):
            return True
        return False

    def launch(self, *args_in: str, **kwargs: Any) -> int:
        args: list[str] = [self.exe]
        args.extend(args_in)
        result = subprocess.run(args)
        return result.returncode

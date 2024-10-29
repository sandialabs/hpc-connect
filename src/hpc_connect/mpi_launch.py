import argparse
import os
import shutil

from .launch import HPCLauncher


class MPILauncher(HPCLauncher):
    name = "mpi"

    def __init__(self) -> None:
        if exe := shutil.which("mpiexec"):
            self._executable = exe
        elif exe := shutil.which("mpirun"):
            self._executable = exe
        else:
            raise ValueError("mpiexec not found on PATH")

    @property
    def executable(self) -> str:
        return self._executable

    @staticmethod
    def matches(name: str) -> bool:
        if name == MPILauncher.name:
            return True
        elif os.path.basename(name) in ("mpirun", "mpiexec"):
            return True
        return False

    def options(self, args: argparse.Namespace, unknown_args: list[str]) -> list[str]:
        """Return options to pass to ``self.executable``"""
        opts: list[str] = []
        opts.extend(unknown_args)
        return opts

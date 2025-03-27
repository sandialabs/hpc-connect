import os
import shutil

from ..hookspec import hookimpl
from ..types import HPCLauncher


class MPILauncher(HPCLauncher):
    def __init__(self, *hints: str, config_file: str | None = None) -> None:
        hints = hints or ("mpiexec", "mpirun")
        for hint in hints:
            path = shutil.which(hint)
            if path is not None:
                break
        else:
            raise ValueError(f"{hints[0]} not found on PATH")
        self._executable = os.fsdecode(path)

    @property
    def executable(self) -> str:
        return self._executable

    @staticmethod
    def matches(arg: str) -> bool:
        if arg == "mpi":
            return True
        elif os.path.basename(arg) in ("mpiexec", "mpirun"):
            return True
        return False


@hookimpl
def hpc_connect_launcher():
    return MPILauncher

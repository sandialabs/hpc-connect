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

    @classmethod
    def factory(self, arg: str, config_file: str | None = None) -> "MPILauncher | None":
        if arg == "mpi":
            return MPILauncher()
        elif os.path.basename(arg) in ("mpiexec", "mpirun"):
            return MPILauncher(arg)
        return None


@hookimpl
def hpc_connect_launcher():
    return MPILauncher

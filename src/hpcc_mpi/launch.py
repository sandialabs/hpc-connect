import logging
import os

import hpc_connect

logger = logging.getLogger("hpc_connect.mpi.launch")


class MPILauncher(hpc_connect.HPCLauncher):
    def __init__(self, config: hpc_connect.Config | None = None) -> None:
        self.config = config or hpc_connect.Config()

    @staticmethod
    def matches(arg: str) -> bool:
        return os.path.basename(arg) in ("mpirun", "mpiexec")

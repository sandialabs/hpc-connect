import logging
import os

import hpc_connect

logger = logging.getLogger("hpc_connect.mpi.launch")


class MPILauncher(hpc_connect.HPCLauncher):
    def __init__(self, config) -> None:
        self.config = config

    @staticmethod
    def matches(arg: str) -> bool:
        return os.path.basename(arg) in ("mpirun", "mpiexec")

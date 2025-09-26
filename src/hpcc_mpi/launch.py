import logging
import os

from hpc_connect.config import Config
from hpc_connect.launch import HPCLauncher

logger = logging.getLogger(__name__)


class MPILauncher(HPCLauncher):
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()

    @staticmethod
    def matches(arg: str) -> bool:
        return os.path.basename(arg) in ("mpirun", "mpiexec")

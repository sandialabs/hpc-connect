from ..config import Config
from . import flux_hl
from . import pbs
from . import shell
from . import slurm
from .base import HPCProcess
from .base import HPCSubmissionFailedError
from .base import HPCSubmissionManager

plugins = (flux_hl, pbs, shell, slurm)


def factory(config: Config | None = None) -> HPCSubmissionManager:
    config = config or Config()
    submission_manager = config.pluginmanager.hook.hpc_connect_submission_manager(config=config)
    if submission_manager is None:
        backend = config.get("submit:backend")
        raise ValueError(f"No matching submission manager for {backend!r}")
    return submission_manager

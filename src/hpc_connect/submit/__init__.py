from typing import Type

from ..config import Config
from .base import HPCProcess
from .base import HPCSubmissionFailedError
from .base import HPCSubmissionManager
from .flux_hl import FluxSubmissionManager
from .pbs import PBSSubmissionManager
from .shell import ShellSubmissionManager
from .slurm import SlurmSubmissionManager

submission_managers: list[Type[HPCSubmissionManager]] = [
    FluxSubmissionManager,  # type: ignore
    PBSSubmissionManager,
    ShellSubmissionManager,
    SlurmSubmissionManager,
]


def factory(config: Config | None = None) -> HPCSubmissionManager:
    config = config or Config()
    backend = config.get("submit:backend")
    for manager_t in submission_managers:
        if manager_t.matches(backend):
            return manager_t(config)
    raise ValueError(f"No matching submission manager for {backend!r}")

from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .launch import SrunLauncher
from .submit import SlurmSubmissionManager

if TYPE_CHECKING:
    from hpc_connect.config import Config
    from hpc_connect.launch import HPCLauncher
    from hpc_connect.submit import HPCSubmissionManager


@hookimpl
def hpc_connect_submission_manager(config: "Config") -> "HPCSubmissionManager | None":
    if SlurmSubmissionManager.matches(config.get("submit:backend")):
        return SlurmSubmissionManager(config=config)
    return None


@hookimpl
def hpc_connect_launcher(config: "Config") -> "HPCLauncher | None":
    if SrunLauncher.matches(config.get("launch:exec")):
        return SrunLauncher(config=config)
    return None

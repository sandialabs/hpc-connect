from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .submit_hl import FluxSubmissionManager

if TYPE_CHECKING:
    from hpc_connect.submit import HPCSubmissionManager


@hookimpl
def hpc_connect_submission_manager(config) -> "HPCSubmissionManager | None":
    if FluxSubmissionManager.matches(config.get("submit:backend")):
        return FluxSubmissionManager(config=config)
    return None

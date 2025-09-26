from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .submit import SubprocessSubmissionManager

if TYPE_CHECKING:
    from hpc_connect.submit import HPCSubmissionManager


@hookimpl
def hpc_connect_submission_manager(config) -> "HPCSubmissionManager | None":
    if SubprocessSubmissionManager.matches(config.get("submit:backend")):
        return SubprocessSubmissionManager(config=config)
    return None

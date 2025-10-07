# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import TYPE_CHECKING
from typing import Any

from hpc_connect.hookspec import hookimpl

from .discover import read_pbsnodes
from .submit import PBSSubmissionManager

if TYPE_CHECKING:
    from hpc_connect.submit import HPCSubmissionManager


@hookimpl
def hpc_connect_submission_manager(config) -> "HPCSubmissionManager | None":
    if PBSSubmissionManager.matches(config.get("submit:backend")):
        return PBSSubmissionManager(config=config)
    return None


@hookimpl
def hpc_connect_discover_resources() -> list[dict[str, Any]] | None:
    if info := read_pbsnodes():
        return info
    return None

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING
from typing import Any

import pluggy

if TYPE_CHECKING:
    from .config import Config
    from .launch import HPCLauncher
    from .submit import HPCSubmissionManager

project_name = "hpc_connect"

hookspec = pluggy.HookspecMarker(project_name)
hookimpl = pluggy.HookimplMarker(project_name)


@hookspec(firstresult=True)
def hpc_connect_submission_manager(config: "Config") -> "HPCSubmissionManager":
    """HPC scheduler implementation"""
    raise NotImplementedError


@hookspec(firstresult=True)
def hpc_connect_launcher(config: "Config") -> "HPCLauncher":
    """HPC scheduler implementation"""
    raise NotImplementedError


@hookspec(firstresult=True)
def hpc_connect_discover_resources() -> list[dict[str, Any]]:
    raise NotImplementedError

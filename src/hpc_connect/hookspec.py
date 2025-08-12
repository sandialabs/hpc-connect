# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING

import pluggy

if TYPE_CHECKING:
    from .config import Config as HPCConfig
    from .launch import HPCLauncher
    from .submit import HPCSubmissionManager


hookspec = pluggy.HookspecMarker("hpc_connect")
hookimpl = pluggy.HookimplMarker("hpc_connect")


@hookspec(firstresult=True)
def hpc_connect_submission_manager(config: "HPCConfig") -> "HPCSubmissionManager | None":
    """HPC scheduler implementation"""


@hookspec(firstresult=True)
def hpc_connect_launcher(config: "HPCConfig") -> "HPCLauncher | None":
    """HPC scheduler implementation"""

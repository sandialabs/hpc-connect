# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING

import pluggy

if TYPE_CHECKING:
    from .backend import Backend
    from .launch import HPCLauncher

project_name = "hpc_connect"

hookspec = pluggy.HookspecMarker(project_name)
hookimpl = pluggy.HookimplMarker(project_name)


@hookspec(firstresult=True)
def hpc_connect_backend(name: str) -> "Backend":
    """HPC scheduler implementation"""
    raise NotImplementedError


@hookspec(firstresult=True)
def hpc_connect_launcher(name: str) -> "HPCLauncher":
    """HPC scheduler implementation"""
    raise NotImplementedError

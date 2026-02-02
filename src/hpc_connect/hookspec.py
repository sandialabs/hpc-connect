# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING

import pluggy

if TYPE_CHECKING:
    from .backend import Backend
    from .config import Config

project_name = "hpc_connect"

hookspec = pluggy.HookspecMarker(project_name)
hookimpl = pluggy.HookimplMarker(project_name)


@hookspec(firstresult=True)
def hpc_connect_backend(config: "Config") -> "Backend":
    """HPC scheduler implementation"""
    raise NotImplementedError

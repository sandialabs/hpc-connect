# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING
from typing import Type

import pluggy

if TYPE_CHECKING:
    from .backend import Backend

project_name = "hpc_connect"

hookspec = pluggy.HookspecMarker(project_name)
hookimpl = pluggy.HookimplMarker(project_name)


@hookspec
def hpc_connect_backend() -> Type["Backend"]:
    """HPC scheduler implementation"""
    raise NotImplementedError

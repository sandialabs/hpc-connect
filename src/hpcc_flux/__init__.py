# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import TYPE_CHECKING
from typing import Any

from hpc_connect.backend import Backend
from hpc_connect.hookspec import hookimpl

from .backend_hl import FluxBackend
from .discover import read_resource_info


@hookimpl
def hpc_connect_backend(name: str) -> "Backend | None":
    if name == "flux":
        return FluxBackend()
    return None

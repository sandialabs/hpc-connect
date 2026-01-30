# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .backend import PBSBackend

if TYPE_CHECKING:
    from hpc_connect.backend import Backend


@hookimpl
def hpc_connect_backend(name: str) -> "Backend | None":
    if name in ("qsub", "pbs"):
        return PBSBackend()
    return None

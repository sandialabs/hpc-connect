# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
from typing import TYPE_CHECKING

import pluggy

if TYPE_CHECKING:
    from ._launch import Namespace

hookspec = pluggy.HookspecMarker("hpc_connect")
hookimpl = pluggy.HookimplMarker("hpc_connect")


@hookspec
def hpc_connect_scheduler():
    """HPC scheduler implementation"""


@hookspec
def hpc_connect_backend():
    """HPC connect backend implementation"""


@hookspec(firstresult=True)
def hpc_connect_launch_join_args(
    args: "Namespace", exec: str, global_options: list[str], local_options: list[str]
) -> list[str] | None:
    """Return the formatted command line"""

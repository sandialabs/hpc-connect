# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import TYPE_CHECKING
from typing import Any

from hpc_connect.hookspec import hookimpl

from .backend import SlurmBackend
from .launch import SrunLauncher

if TYPE_CHECKING:
    from hpc_connect.backend import Backend
    from hpc_connect.launch import HPCLauncher


@hookimpl
def hpc_connect_backend(name: str) -> "Backend | None":
    if name in ("sbatch", "slurm"):
        return SlurmBackend()
    return None


# @hookimpl
# def hpc_connect_launcher(name: str) -> "Backend | None":
#   if SrunLauncher.matches(config.get("launch:exec")):
#       return SrunLauncher(config=config)
#   return None

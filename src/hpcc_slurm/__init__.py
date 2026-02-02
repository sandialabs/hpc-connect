# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import hpc_connect

from .backend import SlurmBackend


@hpc_connect.hookimpl
def hpc_connect_backend(config: hpc_connect.Config) -> "hpc_connect.Backend | None":
    if config.backend in ("sbatch", "slurm"):
        return SlurmBackend(config=config)
    return None

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import hpc_connect

from .backend import RemoteBackend


@hpc_connect.hookimpl
def hpc_connect_backend(config: hpc_connect.Config) -> "hpc_connect.Backend | None":
    if config.backend == "remote_subprocess":
        return RemoteBackend(config=config)
    return None

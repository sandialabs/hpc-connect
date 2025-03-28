# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pluggy

hookspec = pluggy.HookspecMarker("hpc_connect")
hookimpl = pluggy.HookimplMarker("hpc_connect")


@hookspec
def hpc_connect_scheduler():
    """HPC scheduler implementation"""


@hookspec
def hpc_connect_launcher():
    """HPC launcher implementation"""


@hookspec
def hpc_connect_backend():
    """HPC connect backend implementation"""

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
from typing import Type

import hpc_connect

logger = logging.getLogger("hpc_connect.flux")


@hpc_connect.hookimpl(specname="hpc_connect_backend")
def flux_backend() -> Type["hpc_connect.Backend"]:
    from .py import FluxBackend

    return FluxBackend


@hpc_connect.hookimpl(specname="hpc_connect_backend")
def flux_shell_backend() -> Type["hpc_connect.Backend"]:
    from .shell import FluxBackend

    return FluxBackend

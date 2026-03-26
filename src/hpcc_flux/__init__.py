# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
from typing import Type

import hpc_connect

logger = logging.getLogger("hpc_connect.flux")


@hpc_connect.hookimpl
def hpc_connect_backend() -> Type["hpc_connect.Backend"]:
    try:
        from .backend import FluxBackend

        return FluxBackend
    except (ImportError, ModuleNotFoundError) as e:

        class BadFluxBackend(hpc_connect.Backend):
            name = "flux"

            def __init__(self, *args, **kwargs):
                raise RuntimeError(
                    "Flux backed was requested, but the 'flux' Python package "
                    "is not installed or not importable",
                )

        return BadFluxBackend

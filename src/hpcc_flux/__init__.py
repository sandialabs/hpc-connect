# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging

import hpc_connect

logger = logging.getLogger("hpc_connect.flux")


@hpc_connect.hookimpl
def hpc_connect_backend(config: hpc_connect.Config) -> "hpc_connect.Backend | None":
    if config.backend == "flux":
        try:
            from .backend import FluxBackend
        except (ImportError, ModuleNotFoundError) as e:
            logger.error(
                "Flux backed was requested, but the 'flux' Python package "
                "is not installed or not importable",
                exc_info=e,
            )
            return None
        else:
            return FluxBackend(config=config)
    return None

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import Any

import hpc_connect

FluxBackend: type[Any]

try:
    from .backend import FluxBackend as _FluxBackend

    FluxBackend = _FluxBackend

except (ImportError, ModuleNotFoundError):

    class BadFluxBackend(hpc_connect.Backend):
        type = "flux"

        def __init__(self, *args, **kwargs):
            raise RuntimeError(
                "Flux backend was requested, but the 'flux' Python package "
                "is not installed or not importable"
            )

        @classmethod
        def matches(cls, arg: str) -> bool:
            return arg in ("flux", "flux.py", "flux:py")

        @classmethod
        def default_config(cls) -> dict[str, Any]:
            raise RuntimeError(
                "Flux backend was requested, but the 'flux' Python package "
                "is not installed or not importable"
            )

    FluxBackend = BadFluxBackend


__all__ = ["FluxBackend"]

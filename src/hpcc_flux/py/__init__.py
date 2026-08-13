# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

try:
    from .backend import FluxBackend as _FluxBackend

    FluxBackend = _FluxBackend

except (ImportError, ModuleNotFoundError) as e:
    import hpc_connect

    class BadFluxBackend(hpc_connect.Backend):
        type = "flux"

        def __init__(self, *args, **kwargs):
            raise RuntimeError(
                "Flux backed was requested, but the 'flux' Python package "
                "is not installed or not importable"
            )

        @classmethod
        def matches(cls, arg: str) -> bool:
            return arg in ("flux", "flux.py", "flux:py")

    FluxBackend = BadFluxBackend


__all__ = ["FluxBackend"]

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from ..hookspec import hookimpl

try:
    import flux  # noqa: F401

except ImportError:

    class _FluxBackend:
        name = "flux"

        def __init__(self):
            raise RuntimeError("FluxBackend requires the flux module be importable")

        @staticmethod
        def matches(name: str) -> bool:
            return name == "flux"

    @hookimpl
    def hpc_connect_backend():
        return _FluxBackend

else:
    from .flux_api import FluxBackend

    @hookimpl
    def hpc_connect_backend():
        return FluxBackend

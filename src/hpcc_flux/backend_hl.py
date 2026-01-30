# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT


from hpc_connect.backend import Backend

try:
    import flux  # noqa: F401

except ImportError:

    class FluxBackend(Backend):
        name = "flux"

        def __init__(self) -> None:
            raise RuntimeError("FluxSubmissionManager requires the flux module be importable")

        @property
        def resource_specs(self) -> list[dict]:
            raise NotImplementedError

else:
    from .backend_api import FluxBackend  # type: ignore

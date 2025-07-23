# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

try:
    import flux  # noqa: F401

except ImportError:
    from .base import HPCSubmissionManager

    class FluxSubmissionManager(HPCSubmissionManager):
        name = "flux"

        def __init__(self):
            raise RuntimeError("FluxSubmissionManager requires the flux module be importable")

        @staticmethod
        def matches(name: str) -> bool:
            return name == "flux"

else:
    pass

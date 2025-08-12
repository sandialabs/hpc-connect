# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from typing import Any

from ..config import Config
from ..hookspec import hookimpl
from .base import HPCProcess
from .base import HPCSubmissionManager

try:
    import flux  # noqa: F401

except ImportError:

    class FluxSubmissionManager(HPCSubmissionManager):
        name = "flux"

        def __init__(self, config: Config) -> None:
            raise RuntimeError("FluxSubmissionManager requires the flux module be importable")

        def submit(
            self,
            name: str,
            args: list[str],
            scriptname: str | None = None,
            qtime: float | None = None,
            submit_flags: list[str] | None = None,
            variables: dict[str, str | None] | None = None,
            output: str | None = None,
            error: str | None = None,
            nodes: int | None = None,
            cpus: int | None = None,
            gpus: int | None = None,
            **kwargs: Any,
        ) -> HPCProcess:
            raise NotImplementedError

        @staticmethod
        def matches(name: str) -> bool:
            return name == "flux"

else:
    from .flux_api import FluxSubmissionManager  # type: ignore


@hookimpl
def hpc_connect_submission_manager(config) -> HPCSubmissionManager | None:
    if FluxSubmissionManager.matches(config.get("submit:backend")):
        return FluxSubmissionManager(config=config)
    return None

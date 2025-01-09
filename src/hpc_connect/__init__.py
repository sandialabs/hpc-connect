import logging
import os
from typing import Any
from typing import Optional
from typing import Type

from .job import Job
from .launch import HPCLauncher
from .submit import HPCProcess
from .submit import HPCScheduler
from .submit import HPCSubmissionFailedError
from .util import get_entry_points

_launchers: dict[str, Type[HPCLauncher]] | None = None
_schedulers: dict[str, Type[HPCScheduler]] | None = None


def scheduler(name: str) -> HPCScheduler:
    """Return the scheduler matchine ``name``"""
    avail = schedulers()
    for scheduler_t in avail.values():
        if scheduler_t.matches(name):
            return scheduler_t()
    raise ValueError(f"No matching scheduler for {name!r}")


def launcher(name: str) -> HPCLauncher:
    avail = launchers()
    for launcher_t in avail.values():
        if launcher_t.matches(name):
            return launcher_t()
    raise ValueError(f"No matching launcher for {name!r}")


def schedulers() -> dict[str, Type[HPCScheduler]]:
    global _schedulers
    if _schedulers is None:
        from .shell_submit import ShellScheduler

        _schedulers = {ShellScheduler.name: ShellScheduler}
        entry_points = get_entry_points(group="hpc_connect.scheduler") or []
        for entry_point in entry_points:
            try:
                scheduler_t = entry_point.load()
                _schedulers[scheduler_t.name] = scheduler_t
            except ImportError as e:
                _report_failed_plugin(entry_point, e)
    return _schedulers


def launchers() -> dict[str, Type[HPCLauncher]]:
    global _launchers
    if _launchers is None:
        from .mpi_launch import MPILauncher

        _launchers = {MPILauncher.name: MPILauncher}
        entry_points = get_entry_points(group="hpc_connect.launcher") or []
        for entry_point in entry_points:
            try:
                launcher_t = entry_point.load()
                _launchers[launcher_t.name] = entry_point.load()
            except ImportError as e:
                _report_failed_plugin(entry_point, e)
    return _launchers


def _report_failed_plugin(ep, e):
    logger = logging.getLogger("hpc_connect")
    logger.error(
        "\033[1m\033[91m==>\033[0m Error: "
        f"Failed to load HPC connect plugin {ep.name} due to the following error:\n    {e!r}"
    )

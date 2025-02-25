import importlib.metadata as im
from typing import Type

import pluggy

from . import hookspec
from . import mpi_launch
from . import shell_submit
from . import submit
from .job import Job
from .launch import HPCLauncher
from .submit import HPCProcess
from .submit import HPCScheduler
from .submit import HPCSubmissionFailedError

hookimpl = hookspec.hookimpl


manager = pluggy.PluginManager("hpc_connect")
manager.add_hookspecs(hookspec)
manager.register(shell_submit)
manager.register(mpi_launch)
manager.load_setuptools_entrypoints("hpc_connect")


def scheduler(name: str) -> HPCScheduler:
    """Return the scheduler matchine ``name``"""
    for scheduler_t in manager.hook.hpc_connect_scheduler():
        if scheduler_t.matches(name):
            return scheduler_t()
    raise ValueError(f"No matching scheduler for {name!r}")


def schedulers() -> dict[str, HPCScheduler]:
    return {_.name: _ for _ in manager.hook.hpc_connect_scheduler()}


def launcher(name: str) -> HPCLauncher:
    for launcher_t in manager.hook.hpc_connect_launcher():
        if launcher_t.matches(name):
            return launcher_t()
    raise ValueError(f"No matching launcher for {name!r}")


def launchers() -> dict[str, HPCLauncher]:
    return {_.name: _ for _ in manager.hook.hpc_connect_launcher()}

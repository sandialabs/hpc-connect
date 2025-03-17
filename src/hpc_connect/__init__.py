import importlib.metadata as im
import logging
import os
from typing import Type

import pluggy

from . import hookspec
from .impl import builtin
from .job import Job
from .launch import HPCLauncher
from .launch import Parser as LaunchParser
from .submit import HPCProcess
from .submit import HPCScheduler
from .submit import HPCSubmissionFailedError

logger = logging.getLogger("hpc_connect")
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("==> hpc_connect: %(message)s"))
logger.addHandler(ch)
if os.getenv("HPC_CONNECT_DEBUG", "no").lower() in ("yes", "true", "1", "on"):
    ch.setLevel(logging.DEBUG)
    logger.setLevel(logging.DEBUG)


hookimpl = hookspec.hookimpl


manager = pluggy.PluginManager("hpc_connect")
manager.add_hookspecs(hookspec)
for module in builtin:
    manager.register(module)
# manager.load_setuptools_entrypoints("hpc_connect")


def set_debug(arg: bool) -> None:
    if arg:
        for h in logger.handlers:
            h.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)


def scheduler(name: str) -> HPCScheduler:
    """Return the scheduler matchine ``name``"""
    for scheduler_t in manager.hook.hpc_connect_scheduler():
        if scheduler_t.matches(name):
            return scheduler_t()
    raise ValueError(f"No matching scheduler for {name!r}")


def schedulers() -> dict[str, Type[HPCScheduler]]:
    return {_.name: _ for _ in manager.hook.hpc_connect_scheduler()}


def launcher(name: str, config_file: str | None = None) -> HPCLauncher:
    for launcher_t in manager.hook.hpc_connect_launcher():
        if launcher := launcher_t.factory(name, config_file=config_file):
            return launcher
    raise ValueError(f"No matching launcher for {name!r}")


def launchers() -> dict[str, HPCLauncher]:
    return {_.name: _ for _ in manager.hook.hpc_connect_launcher()}

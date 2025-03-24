import importlib.metadata as im
import logging
import os
from typing import Type

import pluggy

from . import hookspec
from .impl import builtin
from .types import HPCBackend
from .types import HPCLauncher
from .types import HPCProcess
from .types import HPCSubmissionFailedError
from .types import LaunchParser

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


def get_backend(arg: str) -> HPCBackend:
    for type in manager.hook.hpc_connect_backend():
        if type.matches(arg):
            return type()
    else:
        raise ValueError(f"invalid backend {arg!r}")


def backends() -> dict[str, Type[HPCBackend]]:
    return {_.name: _ for _ in manager.hook.hpc_connect_backend()}


def launcher(name: str, config_file: str | None = None) -> HPCLauncher:
    for launcher_t in manager.hook.hpc_connect_launcher():
        if launcher := launcher_t.factory(name, config_file=config_file):
            return launcher
    raise ValueError(f"No matching launcher for {name!r}")


def launchers() -> dict[str, HPCLauncher]:
    return {_.name: _ for _ in manager.hook.hpc_connect_launcher()}

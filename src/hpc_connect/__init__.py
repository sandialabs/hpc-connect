# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib.metadata as im
import logging
import os
import subprocess
from typing import Any
from typing import Sequence
from typing import Type

import pluggy

from . import hookspec
from . import pluginmanager
from .types import HPCBackend
from .types import HPCProcess
from .types import HPCSubmissionFailedError

logger = logging.getLogger("hpc_connect")
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("==> hpc_connect: %(message)s"))
logger.addHandler(ch)


loglevelmap: dict[str, int] = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def set_debug(arg: bool) -> None:
    if arg:
        set_logging_level("DEBUG")


def set_logging_level(levelname: str) -> None:
    level = loglevelmap[levelname.upper()]
    for h in logger.handlers:
        h.setLevel(level)
    logger.setLevel(level)


def _initial_logging_setup(*, _ini_setup=[False]):
    if _ini_setup[0]:
        return
    if levelname := os.getenv("HPC_CONNECT_LOG_LEVEL"):
        set_logging_level(levelname)
    else:
        set_logging_level("INFO")
    if os.getenv("HPC_CONNECT_DEBUG", "no").lower() in ("yes", "true", "1", "on"):
        set_logging_level("DEBUG")
    _ini_setup[0] = True


_initial_logging_setup()


hookimpl = hookspec.hookimpl


def get_backend(arg: str) -> HPCBackend:
    for type in pluginmanager.manager.hook.hpc_connect_backend():
        if type.matches(arg):
            return type()
    raise ValueError(f"No matching backend for {arg!r}")


def backends() -> dict[str, Type[HPCBackend]]:
    return {_.name: _ for _ in pluginmanager.manager.hook.hpc_connect_backend()}


def launch(args: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess:
    from . import _launch

    return _launch.launch(args, **kwargs)

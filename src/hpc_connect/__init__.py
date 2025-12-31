# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os

from . import config
from .config import Config
from .config import ConfigScope
from .hookspec import hookimpl
from .launch import HPCLauncher
from .launch import factory as get_launcher
from .logging import get_logger
from .submit import HPCProcess
from .submit import HPCSubmissionFailedError
from .submit import HPCSubmissionManager
from .submit import factory as get_submission_manager

__all__ = [
    "hookimpl",
    "Config",
    "ConfigScope",
    "HPCLauncher",
    "get_launcher",
    "HPCProcess",
    "HPCSubmissionFailedError",
    "HPCSubmissionManager",
    "get_submission_manager",
    "get_logger",
    "HPCBackend",
    "get_backend",
    "Config",
    "ConfigScope",
]


def _initial_logging_setup(*, _ini_setup=[False]):
    from . import logging

    if _ini_setup[0]:
        return
    logging.configure_logging()
    if levelname := os.getenv("HPC_CONNECT_LOG_LEVEL"):
        logging.set_logging_level(levelname)
    else:
        logging.set_logging_level("INFO")
    if os.getenv("HPC_CONNECT_DEBUG", "no").lower() in ("yes", "true", "1", "on"):
        logging.set_logging_level("DEBUG")
    _ini_setup[0] = True


_initial_logging_setup()


# --- backward compatible layer
HPCBackend = HPCSubmissionManager


def get_backend(arg: str) -> HPCSubmissionManager:
    cfg = config.Config()
    cfg.set("submit:backend", arg, scope="internal")
    return get_submission_manager(cfg)

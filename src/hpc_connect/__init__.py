# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os

from . import config
from .config import Config
from .config import ConfigScope
from .hookspec import hookimpl
from .launch import HPCLauncher
from .launch import factory as get_launcher
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
    "HPCBackend",
    "get_backend",
    "Config",
    "ConfigScope",
]

if os.getenv("HPC_CONNECT_DEBUG", "no").lower() in ("yes", "true", "1", "on"):
    logging.getLogger("hpc_connect").setLevel(logging.DEBUG)
elif levelname := os.getenv("HPC_CONNECT_LOG_LEVEL"):
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
    levelno = loglevelmap.get(levelname, logging.NOTSET)
    logging.getLogger("hpc_connect").setLevel(levelno)
else:
    logging.getLogger("hpc_connect").setLevel(logging.NOTSET)


# --- backward compatible layer
HPCBackend = HPCSubmissionManager


def get_backend(arg: str) -> HPCSubmissionManager:
    cfg = config.Config()
    cfg.set("submit:backend", arg, scope="internal")
    return get_submission_manager(cfg)

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os

from .backend import Backend
from .config import Config
from .futures import Future
from .hookspec import hookimpl
from .jobspec import JobSpec
from .launch import HPCLauncher
from .launch import factory as get_launcher
from .pluginmanager import get_backend
from .process import HPCProcess
from .submit import HPCSubmissionManager

__all__ = [
    "Backend",
    "Future",
    "get_backend",
    "get_launcher",
    "hookimpl",
    "HPCLauncher",
    "HPCProcess",
    "HPCSubmissionManager",
    "JobSpec",
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

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os

from . import config
from .launch import factory as get_launcher
from .submit import HPCProcess
from .submit import HPCSubmissionFailedError
from .submit import HPCSubmissionManager
from .submit import factory as get_submission_manager


def _initial_logging_setup(*, _ini_setup=[False]):
    if _ini_setup[0]:
        return
    if levelname := os.getenv("HPC_CONNECT_LOG_LEVEL"):
        config.set_logging_level(levelname)
    else:
        config.set_logging_level("INFO")
    if os.getenv("HPC_CONNECT_DEBUG", "no").lower() in ("yes", "true", "1", "on"):
        config.set_logging_level("DEBUG")
    _ini_setup[0] = True


_initial_logging_setup()


# --- backward compatible layer
HPCBackend = HPCSubmissionManager


def get_backend(arg: str) -> HPCSubmissionManager:
    cfg = config.Config()
    cfg.set("submit:backend", arg, scope="internal")
    return get_submission_manager(cfg)

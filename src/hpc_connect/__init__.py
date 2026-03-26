# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
from typing import Type

from .backend import Backend
from .config import Config
from .config import get_config
from .futures import Future
from .hookspec import hookimpl
from .jobspec import JobSpec
from .launch import HPCLauncher
from .launch import LaunchAdapter
from .process import HPCProcess
from .submit import HPCSubmissionManager

__all__ = [
    "Backend",
    "Config",
    "get_config",
    "Future",
    "get_backend",
    "hookimpl",
    "HPCLauncher",
    "LaunchAdapter",
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


def get_backend(arg: str | None = None) -> Backend:
    import copy

    from .config import get_config
    from .pluginmanager import get_pluginmanager
    from .schemas import backend_schema
    from .util import collections

    config = get_config()
    name: str
    if arg is not None:
        name = arg
    elif default_backend := config.get("backend"):
        name = default_backend
    elif len(config["backends"]) == 1:
        b = config["backends"][0]
        name = b.get("name", b["type"])
    else:
        raise TypeError("missing required argument: 'arg'")

    for entry in config["backends"]:
        if entry.get("name") == name:
            type = entry["type"]
            break
        elif entry["type"] == name:
            type = entry["type"]
            break
    else:
        type = name

    pm = get_pluginmanager()
    backend_t: Type[Backend] | None
    for backend_t in pm.hook.hpc_connect_backend():
        if backend_t is not None and backend_t.matches(type):
            break
    else:
        raise ValueError(f"{type}: backend not registered with hpc_connect")

    # Make the config for the backend
    defaults = copy.deepcopy(backend_t.default_config())
    backend_config = backend_schema.validate(defaults)

    if overrides := config.backend(name):
        collections.merge(backend_config, overrides)
        backend_config = backend_schema.validate(backend_config)

    return backend_t(cfg=backend_config)


def backends() -> list[str]:
    from .pluginmanager import get_pluginmanager

    pm = get_pluginmanager()
    return [b.name for b in pm.hook.hpc_connect_backend() if b is not None]

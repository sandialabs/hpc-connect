# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import logging as builtin_logging
import sys


def get_logger(name: str):
    parts = name.split(".")
    if parts[0] != "hpc_connect":
        parts.insert(0, "hpc_connect")
    return builtin_logging.getLogger(".".join(parts))


loglevelmap: dict[str, int] = {
    "CRITICAL": builtin_logging.CRITICAL,
    "FATAL": builtin_logging.FATAL,
    "ERROR": builtin_logging.ERROR,
    "WARN": builtin_logging.WARNING,
    "WARNING": builtin_logging.WARNING,
    "INFO": builtin_logging.INFO,
    "DEBUG": builtin_logging.DEBUG,
    "NOTSET": builtin_logging.NOTSET,
}


def set_debug(arg: bool) -> None:
    if arg:
        set_logging_level("DEBUG")


def set_logging_level(levelname: str) -> None:
    logger = builtin_logging.getLogger("hpc_connect")
    level = loglevelmap[levelname.upper()]
    for h in logger.handlers:
        h.setLevel(level)
    logger.setLevel(level)


def configure_logging(levelname: str = "WARNING"):
    logger = builtin_logging.getLogger("hpc_connect")
    if not logger.handlers:
        sh = builtin_logging.StreamHandler(sys.stderr)
        fmt = builtin_logging.Formatter("%(levename)s: %(module)s::%(funcName)s: %(message)s")
        sh.setFormatter(fmt)

    levelno = loglevelmap[levelname.upper()]
    for h in logger.handlers:
        h.setLevel(levelno)
    logger.setLevel(levelno)

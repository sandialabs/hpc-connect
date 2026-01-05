# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import logging
import sys

from . import command


def main(argv: list[str] | None = None) -> int:
    logger = logging.getLogger("hpc_connect")
    if not logger.handlers:
        sh = logging.StreamHandler(sys.stderr)
        fmt = logging.Formatter("%(levename)s: %(module)s::%(funcName)s: %(message)s")
        sh.setFormatter(fmt)
        sh.setLevel(logging.INFO)
        logger.addHandler(sh)
    argv = argv or sys.argv[1:]
    return command.main(argv)


def launch(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    argv.insert(0, "launch")
    return command.main(argv)


if __name__ == "__main__":
    sys.exit(main())

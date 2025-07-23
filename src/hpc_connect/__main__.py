# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import sys

from . import command


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    return command.main(argv)


def launch(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    argv.insert(0, "launch")
    return command.main(argv)


if __name__ == "__main__":
    sys.exit(main())

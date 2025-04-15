# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import sys

import hpc_connect


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    print("HPC Connect backends:")
    for backend_t in hpc_connect.backends().values():
        print(f"- {backend_t.name}")
    return 0


def launch_main(argv: list[str] | None = None) -> None:
    from . import _launch

    args = _launch.format_command_line(argv or sys.argv[1:])
    os.execvp(args[0], args)


if __name__ == "__main__":
    sys.exit(main())

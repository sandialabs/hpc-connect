# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
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
    from . import config

    argv = argv or sys.argv[1:]
    p = argparse.ArgumentParser(
        add_help=False,
        usage="%(prog)s [--help] ...",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "-h",
        "--help",
        default=False,
        action="help",
        help="Show this message and exit.  Use -H to display the backends help page.",
    )
    p.epilog = _launch.__doc__
    p.parse_known_args(argv)

    if "-H" in argv:
        argv[argv.index("-H")] = "-h"

    parser = _launch.ArgumentParser(
        mappings=config.get("launch:mappings"), numproc_flag=config.get("launch:numproc_flag")
    )
    args = parser.parse_args(argv)
    cmd = _launch.join_args(args)
    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    sys.exit(main())

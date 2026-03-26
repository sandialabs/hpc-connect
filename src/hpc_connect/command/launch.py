# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import os
import shlex
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse

    from ..config import Config

description = "Wrapper to launch tool, such as mpiexec or srun"
add_help = False


def setup_parser(parser: "argparse.ArgumentParser") -> None:
    parser.usage = "%(prog)s [-h] [--help] ..."
    parser.add_argument(
        "-h",
        default=False,
        action="help",
        help="Show this message and exit.  For the backend's help, run 'hpcc launch --help'",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        default=False,
        help="Print command line to shell but do not execute",
    )


def execute(config: "Config", args: "argparse.Namespace") -> None:
    from .. import get_backend

    backend = get_backend(config["backend"])
    launcher = backend.launcher()
    cmd = launcher.adapter.build_argv(list(args.extra_args))
    if args.dryrun:
        print(shlex.join(cmd))
        return
    os.execvp(cmd[0], cmd)  # nosec B606

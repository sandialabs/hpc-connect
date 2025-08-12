# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import os
import shlex

from ..config import Config
from ..launch import factory as get_launcher

description = "Wrapper to launch tool, such as mpiexec or srun"
add_help = False


def setup_parser(parser: argparse.ArgumentParser) -> None:
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


def execute(config: Config, args: argparse.Namespace) -> None:
    argv = list(args.extra_args)
    launcher = get_launcher(config)
    cmd = launcher.prepare_command_line(argv)
    if args.dryrun:
        print(shlex.join(cmd))
        return
    os.execvp(cmd[0], cmd)

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT
import argparse
import os
import shlex

from ..config import Config

description = "Wrapper to submit tool, such as sbatch"
add_help = False


def setup_parser(parser: argparse.ArgumentParser) -> None:
    from .. import submit

    parser.usage = "%(prog)s [-h] [--help] ..."
    parser.add_argument(
        "-h",
        default=False,
        action="help",
        help="Show this message and exit.",
    )
    parser.add_argument(
        "--dryrun",
        action="store_true",
        default=False,
        help="Print command line to shell but do not execute",
    )
    parser.formatter_class = argparse.RawDescriptionHelpFormatter
    parser.epilog = submit.__doc__


def execute(config: Config, args: argparse.Namespace) -> None:
    import hpc_connect

    argv = list(args.extra_args)
    manager = hpc_connect.get_submission_manager(config)
    cmd = manager.prepare_command_line(argv)
    if args.dryrun:
        print(shlex.join(cmd))
        return
    os.execvp(cmd[0], cmd)

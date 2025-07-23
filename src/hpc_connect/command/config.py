import argparse
import sys

from ..config import Config

description = "Show, get, and set config values"


def setup_parser(parser: argparse.ArgumentParser) -> None:
    parent = parser.add_subparsers(dest="subcommand")
    p = parent.add_parser("show")
    p.add_argument(
        "--scope",
        default=None,
        help="Add settings to this config scope [default: %(default)s]",
    )
    p = parent.add_parser("add")
    p.add_argument(
        "--scope",
        choices=("site", "global", "local"),
        default="local",
        help="Add settings to this config scope [default: %(default)s]",
    )
    p.add_argument(
        "add_config_paths",
        nargs="+",
        metavar="path",
        help="colon-separated path to config that should be set, e.g. 'config:default:true'",
    )


def execute(config: Config, args: argparse.Namespace) -> None:
    if args.subcommand == "show":
        config.dump(sys.stdout, scope=args.scope, default_flow_style=False)
    elif args.subcommand == "add":
        for path in args.add_config_paths:
            config.add(path, scope=args.scope)

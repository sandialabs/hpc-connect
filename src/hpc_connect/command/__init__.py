import argparse
import sys
from types import ModuleType

from ..config import Config
from . import config
from . import launch
from . import submit

_commands: dict[str, ModuleType] = {}


def main(argv: list[str] | None = None) -> int:
    parser = make_parser()
    args, extra_args = parser.parse_known_args(argv or sys.argv[1:])
    args.extra_args = extra_args

    module = _commands[args.command]
    cfg = Config()
    cfg.set_main_options(args)
    module.execute(cfg, args)
    return 0


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        dest="config_mods",
        action="append",
        metavar="path",
        help="colon-separated path to config that should be set, e.g. 'config:default:true'",
    )
    subparsers = parser.add_subparsers(dest="command")
    add_command(subparsers, config)
    add_command(subparsers, launch)
    add_command(subparsers, submit)
    return parser


def add_command(subparsers: argparse._SubParsersAction, module: ModuleType) -> None:
    name = module.__name__.split(".")[-1].lower()
    parser = subparsers.add_parser(
        name, add_help=getattr(module, "add_help", True), help=module.description
    )
    module.setup_parser(parser)
    _commands[name] = module

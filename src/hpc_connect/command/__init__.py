"""
Overview
--------

`hpcc` is a lightweight and configurable wrapper around HPC schedulers and program launchers like
`mpiexec` or `srun`. `hpcc` provides a single interface to multiple backends, simplifying the
process of running jobs in an HPC environment.  backend implementation.

Configuration
-------------

The default behavior of `hpcc` can be changed by providing a yaml configuration file.  The default
configuration is:

.. code-block:: yaml

   hpc_connect:
     debug: false
     backend: my.backend
     backends:
     - name: my.backend
       type: local
       submit:
         default_options: []
       launch:
         name: openmpi
         type: mpi
         exec: mpiexec
         numproc_flag: -n  # Flag to pass to the backend before giving it the number of processors to run on.
         default_options: []  # Options to pass to the backend before any other arguments.
         pre_options: []  # Command line options placed immediately before program to run
         mpmd:
           global_options: []
           local_options: []

Configurations are read from:

1. Local configuration: ./hpc_connect.yaml
2. Global configuration [1]: ~/.config/hpc_connect/config.yaml
3. Site configuration [2]: sys.prefix/etc/hpc_connect/config.yaml

[1] The global configuration will be read from the HPC_CONNECT_GLOBAL_CONFIG environment variable, if set
[2] The site configuration will be read from the HPC_CONNECT_SITE_CONFIG environment variable, if set

"""

import argparse
import sys
from types import ModuleType

from ..config import Config
from . import config
from . import launch

_commands: dict[str, ModuleType] = {}


def main(argv: list[str] | None = None) -> int:
    parser = make_parser()
    args, extra_args = parser.parse_known_args(argv or sys.argv[1:])
    if args.info:
        print(__doc__)
        return 0
    args.extra_args = extra_args

    module = _commands[args.command]
    cfg = Config()
    cfg.set_main_options(args)
    module.execute(cfg, args)  # ty: ignore[unresolved-attribute]
    return 0


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--info", action="store_true", help="Show additional information and exit.")
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
    return parser


def add_command(subparsers: argparse._SubParsersAction, module: ModuleType) -> None:
    name = module.__name__.split(".")[-1].lower()
    add_help = getattr(module, "add_help", True)
    description = getattr(module, "description", None)
    parser = subparsers.add_parser(name, add_help=add_help, help=description)
    module.setup_parser(parser)  # ty: ignore[unresolved-attribute]
    _commands[name] = module

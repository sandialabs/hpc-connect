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
import importlib.resources as ir
import importlib.util
import os
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
    module.execute(cfg, args)
    return 0


def make_parser() -> argparse.ArgumentParser:
    from .. import version as _v

    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--info", action="store_true", help="Show additional information and exit.")
    parser.add_argument(
        "-c",
        dest="config_mods",
        action="append",
        metavar="path",
        help="colon-separated path to config that should be set, e.g. 'config:default:true'",
    )
    parser.add_argument(
        "--version", action="version", version=_v.version, help="Show hpc connect version and exit"
    )
    subparsers = parser.add_subparsers(dest="command")
    add_command(subparsers, config)
    add_command(subparsers, launch)
    _load_dev_command(subparsers)
    return parser


def add_command(subparsers: argparse._SubParsersAction, module: ModuleType) -> None:
    name = getattr(module, "command_name", None) or module.__name__.split(".")[-1].lower()
    add_help = getattr(module, "add_help", True)
    description = getattr(module, "description", None)
    parser = subparsers.add_parser(name, add_help=add_help, help=description)
    module.setup_parser(parser)
    _commands[name] = module


def _load_dev_command(subparsers: argparse._SubParsersAction) -> None:
    """Load ``dev/pre_commit.py`` when running from an editable checkout.

    Detection: resolve the ``hpc_connect`` package root via
    ``importlib.resources``, walk one level up to the repo root, and check
    for both a ``.git/`` directory (editable install) and a
    ``dev/pre_commit.py`` file.  If both are present the module is loaded
    with ``importlib.util`` and registered as a subcommand exactly as if it
    had been passed to :func:`add_command`.

    This is intentionally silent: if either condition is not met (installed
    release, no ``.git``, or no ``dev/pre_commit.py``) the function does
    nothing.
    """
    try:
        pkg_path = str(ir.files("hpc_connect"))
        repo_root = os.path.normpath(os.path.join(pkg_path, "../.."))
    except Exception:
        return

    if not os.path.isdir(os.path.join(repo_root, ".git")):
        return

    dev_file = os.path.join(repo_root, "dev", "pre_commit.py")
    if not os.path.isfile(dev_file):
        return

    try:
        spec = importlib.util.spec_from_file_location("hpc_connect_dev.pre_commit", dev_file)
        if spec is None or spec.loader is None:
            return
        mod: ModuleType = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        add_command(subparsers, mod)
    except Exception as exc:
        import warnings

        warnings.warn(f"Failed to load developer command from {dev_file!r}: {exc}", stacklevel=2)


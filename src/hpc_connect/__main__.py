import argparse
import sys
from typing import Optional

import hpc_connect

from .launch import HPCLauncher


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    print("HPC Connect schedulers:")
    for scheduler_t in hpc_connect.schedulers():
        print(f"- {scheduler_t.name}")
    print()
    print("HPC Connect launchers:")
    for launcher_t in hpc_connect.launchers():
        print(f"- {launcher_t.name}")
    return 0


def launch(argv: Optional[list[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="hpc-launch", description="Abstract HPC launch interface"
    )
    parser.add_argument("--backend", help="Launch with this backend")
    args, extra_args = parser.parse_known_args(argv)
    if args.backend:
        hpc_connect.set(launcher=args.backend)
    launcher: HPCLauncher = hpc_connect.launcher
    return launcher.launch(*extra_args)


if __name__ == "__main__":
    sys.exit(main())

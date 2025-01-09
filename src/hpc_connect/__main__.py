import argparse
import os
import sys
from typing import Optional

import hpc_connect


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv or sys.argv[1:]
    print("HPC Connect schedulers:")
    for scheduler_t in hpc_connect.schedulers().values():
        print(f"- {scheduler_t.name}")
    print()
    print("HPC Connect launchers:")
    for launcher_t in hpc_connect.launchers().values():
        print(f"- {launcher_t.name}")
    return 0


def launch(argv: Optional[list[str]] = None):
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(
        prog="hpc-launch", description="Abstract HPC launch interface"
    )
    parser.add_argument("--backend", help="Launch with this backend [default: mpi]")
    ns, unknown_args = parser.parse_known_args(argv)
    name: str = "mpi"
    if ns.backend:
        name = ns.backend
    elif "HPC_CONNECT_PREFERRED_LAUNCHER" in os.environ:
        name = os.environ["HPC_CONNECT_PREFERRED_LAUNCHER"]
    launcher = hpc_connect.launcher(name)
    exe = os.fsdecode(launcher.executable)
    opts = launcher.options(ns, unknown_args)
    if sys.platform == "win32":
        import subprocess

        completed_process = subprocess.run([exe, *opts])
        sys.exit(completed_process.returncode)
    else:
        os.execvp(exe, [exe, *opts])


if __name__ == "__main__":
    sys.exit(main())

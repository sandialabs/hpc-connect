# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import subprocess
import sys

import hpc_connect


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    print("HPC Connect backends:")
    for backend_t in hpc_connect.backends().values():
        print(f"- {backend_t.name}")
    print()
    print("HPC Connect launchers:")
    for launcher_t in hpc_connect.launchers().values():
        print(f"- {launcher_t.name}")
    return 0


def launch(argv: list[str] | None = None) -> None:
    parser = hpc_connect.LaunchParser(prog="hpc-launch")

    pre, prog_args = parser.preparse(argv or sys.argv[1:])

    if pre.help:
        print(pre)
        if pre.backend is None and "HPC_CONNECT_LAUNCHER" not in os.environ:
            parser.print_help()
            sys.exit(0)
        else:
            prog_args.insert(0, "-h")

    name: str = "mpi"
    if pre.backend:
        name = pre.backend
    elif "HPC_CONNECT_LAUNCHER" in os.environ:
        name = os.environ["HPC_CONNECT_LAUNCHER"]
    elif "HPC_CONNECT_PREFERRED_LAUNCHER" in os.environ:
        name = os.environ["HPC_CONNECT_PREFERRED_LAUNCHER"]

    launcher = hpc_connect.get_launcher(name, config_file=pre.config_file)
    ns = launcher.inspect_args(prog_args)
    args = launcher.format_program_args(ns)
    exe = os.fsdecode(launcher.executable)
    completed_process = subprocess.run([exe, *args])
    sys.exit(completed_process.returncode)


if __name__ == "__main__":
    sys.exit(main())

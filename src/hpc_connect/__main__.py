import os
import subprocess
import sys

import hpc_connect


def main(argv: list[str] | None = None) -> int:
    argv = argv or sys.argv[1:]
    print("HPC Connect schedulers:")
    for scheduler_t in hpc_connect.schedulers().values():
        print(f"- {scheduler_t.name}")
    print()
    print("HPC Connect launchers:")
    for launcher_t in hpc_connect.launchers().values():
        print(f"- {launcher_t.name}")
    return 0


def launch(argv: list[str] | None = None) -> None:
    parser = hpc_connect.LaunchParser(prog="hpc-launch")
    pre, local_options, prog_options = parser.preparse(argv)
    name: str = "mpi"
    if pre.backend:
        name = pre.backend
    elif "HPC_CONNECT_PREFERRED_LAUNCHER" in os.environ:
        name = os.environ["HPC_CONNECT_PREFERRED_LAUNCHER"]
    launcher = hpc_connect.launcher(name, config_file=pre.config_file)
    launcher.setup_parser(parser)
    parser.parse_args(local_options, namespace=pre)
    launcher.set_main_options(pre)
    ns = launcher.inspect_args(prog_options)
    if ns.help:
        parser.print_help()
        sys.exit(0)
    args = launcher.format_program_args(ns)
    exe = os.fsdecode(launcher.executable)
    completed_process = subprocess.run([exe, *args])
    sys.exit(completed_process.returncode)


if __name__ == "__main__":
    sys.exit(main())

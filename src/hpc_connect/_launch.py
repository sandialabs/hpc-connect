import os
import shlex
import shutil
import subprocess
from typing import Any
from typing import Sequence

import yaml


class LaunchArgs:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)


class LaunchConfig:
    def __init__(self) -> None:
        self.config: dict[str, Any] = {"exec": "mpiexec", "default_flags": [], "numproc_flag": "-n"}
        if x := os.getenv("HPCC_LAUNCH_EXEC"):
            self.config["exec"] = x
        if x := os.getenv("HPCC_LAUNCH_DEFAULT_FLAGS"):
            self.config["default_flags"] = shlex.split(x)
        if x := os.getenv("HPCC_LAUNCH_NUMPROC_FLAG"):
            self.config["numproc_flag"] = x
        file: str
        if "HPCC_CONFIG_FILE" in os.environ:
            file = os.environ["HPCC_CONFIG_FILE"]
        elif "XDG_CONFIG_HOME" in os.environ:
            file = os.path.join(os.environ["XDG_CONFIG_HOME"], "hpc_connect/config.yaml")
        else:
            file = os.path.expanduser("~/.config/hpc_connect/config.yaml")
        if os.path.exists(file):
            with open(file) as fh:
                config = yaml.safe_load(fh)
            launch_config = config["hpc_connect"].get("launch", {})
            if "exec" in launch_config:
                self.config["exec"] = launch_config["exec"]
            if "default_flags" in launch_config:
                self.config["default_flags"] = shlex.split(launch_config["default_flags"])
            if "numproc_flag" in launch_config:
                self.config["numproc_flag"] = launch_config["numproc_flag"]

    @property
    def exec(self) -> str:
        path = shutil.which(self.config["exec"])
        if path is None:
            raise ValueError(f"{self.config['exec']}: executable not found")
        return path

    @property
    def default_flags(self) -> list[str]:
        return self.config["default_flags"]

    @property
    def numproc_flag(self) -> str:
        return self.config["numproc_flag"]


def inspect_args(args: Sequence[str], config: LaunchConfig | None = None) -> LaunchArgs:
    """Inspect arguments to launch to infer number of processors requested"""
    config = config or LaunchConfig()
    numproc_flags = {"-n", "--n", "-np", "--np"}
    numproc_flags.add(config.numproc_flag)
    numproc_long_flags = ("--n=", "--np=")

    la = LaunchArgs()

    spec: list[str] = []
    processes: int | None = None
    command_seen: bool = False

    iter_args = iter(args or [])
    while True:
        try:
            arg = next(iter_args)
        except StopIteration:
            break
        if shutil.which(arg):
            command_seen = True
        if not command_seen:
            if arg in numproc_flags:
                s = next(iter_args)
                processes = int(s)
                spec.extend([config.numproc_flag, s])
            elif arg.startswith(numproc_long_flags):
                _, _, s = arg.partition("=")
                processes = int(s)
                spec.append(arg)
            else:
                spec.append(arg)
        elif arg == ":":
            # MPMD: end of this segment
            la.add(spec, processes)
            spec.clear()
            command_seen, processes = False, None
        else:
            spec.append(arg)

    if spec:
        la.add(spec, processes)

    return la


def expand(arg: str, config: LaunchConfig, **kwargs: Any) -> str:
    kwargs["numproc_flag"] = config.numproc_flag
    return arg % kwargs


def format_command_line(args_in: Sequence[str], config: LaunchConfig | None = None) -> list[str]:
    config = config or LaunchConfig()
    args = inspect_args(args_in, config=config)
    cmd = [os.fsdecode(config.exec)]
    np = sum(p for p in args.processes if p)
    for default_arg in config.default_flags:
        cmd.append(expand(default_arg, config, np=np))
    for i, spec in enumerate(args.specs):
        for arg in spec:
            cmd.append(expand(arg, config, np=args.processes[i]))
        cmd.append(":")
    if cmd[-1] == ":":
        cmd.pop()  # remove trailing :
    return cmd


def launch(
    args_in: Sequence[str], config: LaunchConfig | None = None, **kwargs: Any
) -> subprocess.CompletedProcess:
    config = config or LaunchConfig()
    args = format_command_line(args_in, config=config)
    return subprocess.run(args, **kwargs)

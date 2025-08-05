import logging
import os
import shlex
import shutil
import subprocess
from typing import Any
from typing import Generator
from typing import Sequence

from ..config import Config
from ..hookspec import hookimpl


class HPCLauncher:
    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()

    def __call__(
        self, args: list[str], echo: bool = False, **kwargs: Any
    ) -> subprocess.CompletedProcess:
        cmd = self.prepare_command_line(args)
        if echo:
            logging.getLogger(__name__).log(100, f"Command line: {shlex.join(cmd)}")
        return subprocess.run(cmd, **kwargs)

    @staticmethod
    def matches(arg: str) -> bool:
        return True

    def get_from_config(self, key: str, default: Any | None = None) -> Any:
        myexec = self.exec
        if value := self.config.get(f"launch:{myexec}:{key}"):
            return value
        elif value := self.config.get(f"launch:{os.path.basename(myexec)}:{key}"):
            return value
        else:
            value = self.config.get(f"launch:{key}")
            return default if value is None else value

    def set_config_var(self, key: str, value: Any, scope: str | None = None) -> None:
        myexec = self.exec
        self.config.set(f"launch:{os.path.basename(myexec)}:{key}", value, scope=scope)

    @property
    def exec(self) -> str:
        return self.config.get("launch:exec")

    @property
    def mappings(self) -> dict[str, str]:
        return self.get_from_config("mappings") or {}

    @property
    def numproc_flag(self) -> str:
        return self.get_from_config("numproc_flag", default="-n")

    @property
    def local_options(self) -> list[str]:
        return self.get_from_config("local_options") or []

    @property
    def global_options(self) -> list[str]:
        return self.get_from_config("default_options") or []

    @property
    def pre_options(self) -> list[str]:
        return self.get_from_config("pre_options") or []

    def prepare_command_line(self, args: list[str]) -> list[str]:
        parser = ArgumentParser(mappings=self.mappings, numproc_flag=self.numproc_flag)
        launchspecs = parser.parse_args(args)
        return self.join_specs(launchspecs)

    def join_specs(
        self,
        launchspecs: "LaunchSpecs",
        local_options: list[str] | None = None,
        global_options: list[str] | None = None,
        pre_options: list[str] | None = None,
    ) -> list[str]:
        local_options = list(local_options or [])
        local_options.extend(self.local_options)
        global_options = list(global_options or [])
        global_options.extend(self.global_options)
        pre_options = list(pre_options or [])
        pre_options.extend(self.pre_options)

        cmd = [os.fsdecode(self.exec)]
        np = sum(p for p in launchspecs.processes if p)
        required_resources = self.config.compute_required_resources(ranks=np)
        for opt in global_options:
            cmd.append(self.expand(opt, **required_resources))
        for p, spec in launchspecs:
            for opt in local_options:
                cmd.append(self.expand(opt, np=p))
            i = argp(spec)
            if i == -1:
                launch_opts, program_opts = [], spec
            else:
                launch_opts, program_opts = spec[:i], spec[i:]
            for opt in launch_opts:
                cmd.append(self.expand(opt, np=p))
            for opt in pre_options:
                cmd.append(self.expand(opt, np=p))
            for opt in program_opts:
                cmd.append(self.expand(opt, np=p))
            cmd.append(":")
        if cmd[-1] == ":":
            cmd.pop()
        return cmd

    @staticmethod
    def expand(arg: str, **kwargs: Any) -> str:
        return str(arg) % kwargs


class LaunchSpecs:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []

    def __len__(self) -> int:
        return len(self.specs)

    def __repr__(self) -> str:
        s = " : ".join(shlex.join(_) for _ in self.specs)
        return f"LaunchSpecs({s})"

    def __iter__(self) -> Generator[tuple[int | None, list[str]], None, None]:
        for i, spec in enumerate(self.specs):
            yield self.processes[i], spec

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)


class ArgumentParser:
    def __init__(self, *, mappings: dict[str, str] | None, numproc_flag: str = "-n") -> None:
        self.mappings: dict[str, str] = mappings or {}
        self.numproc_flag = numproc_flag
        if "-n" not in self.mappings:
            # always map -n to numproc_flag
            self.mappings["-n"] = self.numproc_flag

    def mapped(self, arg: str) -> str | None:
        if arg in self.mappings:
            return self.mappings[arg]
        # check for the case of long opt: arg=
        for pat, repl in self.mappings.items():
            if arg.startswith(f"{pat}="):
                return arg.replace(pat, repl)
        return None

    def parse_args(self, args: Sequence[str]) -> LaunchSpecs:
        """Inspect arguments to launch to infer number of processors requested"""
        launchspecs = LaunchSpecs()
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
                if new := self.mapped(arg):
                    if new.startswith("SUPPRESS="):
                        continue
                    elif new == "SUPPRESS":
                        next(iter_args)
                        continue
                    arg = new
                if arg == self.numproc_flag:
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif arg.startswith(f"{self.numproc_flag}="):
                    i = len(self.numproc_flag) + 1
                    processes = int(arg[i:])
                    spec.append(arg)
                else:
                    spec.append(arg)
            elif arg == ":":
                # MPMD: end of this segment
                launchspecs.add(spec, processes)
                spec.clear()
                command_seen, processes = False, None
            else:
                spec.append(arg)

        if spec:
            launchspecs.add(spec, processes)

        return launchspecs


def argp(args: list[str]) -> int:
    for i, arg in enumerate(args):
        if shutil.which(arg):
            return i
    return -1


@hookimpl(trylast=True)
def hpc_connect_launcher(config: Config) -> HPCLauncher:
    return HPCLauncher(config=config)

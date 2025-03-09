import abc
import argparse
import shutil
import sys

from .util import partition


class Parser(argparse.ArgumentParser):
    def __init__(self, **kwargs):
        kwargs["add_help"] = False
        super().__init__(**kwargs)
        self.add_argument("--backend", help="Use this launcher backend [default: mpiexec]")
        self.add_argument("--config-file", help="Use this configuration file")

    def preparse(
        self, argv: list[str] | None = None
    ) -> tuple[argparse.Namespace, list[str], list[str]]:
        l_opts, g_opts = partition(argv or sys.argv[1:], lambda x: x.startswith("-Wl,"))
        args, l_opts = self.parse_known_args([_[4:] for _ in l_opts])
        return args, l_opts, g_opts


class LaunchArgs:
    def __init__(self) -> None:
        self.specs: list[list[str]] = []
        self.processes: list[int | None] = []
        self.help: bool = False

    def add(self, spec: list[str], processes: int | None) -> None:
        self.specs.append(list(spec))
        self.processes.append(processes)

    def empty(self) -> bool:
        return len(self.specs) == 0


class HPCLauncher(abc.ABC):
    name = "<launcher>"

    def __init__(self, *hints: str, config_file: str | None = None) -> None:
        pass

    @property
    @abc.abstractmethod
    def executable(self) -> str: ...

    @classmethod
    @abc.abstractmethod
    def factory(self, arg: str, config_file: str | None = None) -> "HPCLauncher | None": ...

    def setup_parser(self, parser: Parser) -> None:
        pass

    def set_main_options(self, args: argparse.Namespace) -> None:
        pass

    def default_args(self) -> list[str]:
        return []

    def inspect_args(self, args: list[str]) -> LaunchArgs:
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
                if la.empty() and arg in ("-h", "--help"):
                    la.help = True
                if arg in ("-n", "--n", "-np", "--np", "-c"):
                    s = next(iter_args)
                    processes = int(s)
                    spec.extend([arg, s])
                elif arg.startswith(("--n=", "--np=")):
                    arg, _, s = arg.partition("=")
                    processes = int(s)
                    spec.extend([arg, s])
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

    def format_program_args(self, args: LaunchArgs) -> list[str]:
        specs = list(self.default_args())
        for i, arg in enumerate(specs):
            specs[i] = arg % {"np": args.processes[i]}
        specs.extend(args.specs[0])
        for spec in args.specs[1:]:
            specs.append(":")
            specs.extend(spec)
        return specs

import importlib.metadata as im
from typing import Type

from .job import Job
from .launch import HPCLauncher
from .submit import HPCProcess
from .submit import HPCScheduler
from .submit import HPCSubmissionFailedError


class EntryPointLoadFailure:
    def __init__(self, ep: im.EntryPoint, ex: Exception) -> None:
        self.ep = ep
        self.ex = ex
        self.name = f"{self.ep.name}*"

    def __repr__(self) -> str:
        return f"Failed to load {self.ep!r} due to: {self.ex!r}"

    def matches(self, name: str) -> bool:
        return name.lower() == self.ep.name.lower()

    def __call__(self):
        raise RuntimeError(repr(self))


_launchers: dict[str, Type[HPCLauncher] | EntryPointLoadFailure] | None = None
_schedulers: dict[str, Type[HPCScheduler] | EntryPointLoadFailure] | None = None


def scheduler(name: str) -> HPCScheduler:
    """Return the scheduler matchine ``name``"""
    avail = schedulers()
    for scheduler_t in avail.values():
        if scheduler_t.matches(name):
            return scheduler_t()
    raise ValueError(f"No matching scheduler for {name!r}")


def launcher(name: str) -> HPCLauncher:
    avail = launchers()
    for launcher_t in avail.values():
        if launcher_t.matches(name):
            return launcher_t()
    raise ValueError(f"No matching launcher for {name!r}")


def schedulers() -> dict[str, Type[HPCScheduler] | EntryPointLoadFailure]:
    global _schedulers
    if _schedulers is None:
        from .shell_submit import ShellScheduler

        hooks: dict[str, Type[HPCScheduler] | EntryPointLoadFailure] = {
            ShellScheduler.name: ShellScheduler
        }
        entry_points = im.entry_points(group="hpc_connect.scheduler")
        for entry_point in entry_points:
            try:
                hooks[entry_point.name] = entry_point.load()
            except ImportError as e:
                hooks[entry_point.name] = EntryPointLoadFailure(entry_point, e)
        _schedulers = hooks
    return _schedulers


def launchers() -> dict[str, Type[HPCLauncher] | EntryPointLoadFailure]:
    global _launchers
    if _launchers is None:
        from .mpi_launch import MPILauncher

        hooks: dict[str, Type[HPCLauncher] | EntryPointLoadFailure] = {
            MPILauncher.name: MPILauncher
        }
        entry_points = im.entry_points(group="hpc_connect.launcher")
        for entry_point in entry_points:
            try:
                hooks[entry_point.name] = entry_point.load()
            except ImportError as e:
                hooks[entry_point.name] = EntryPointLoadFailure(entry_point, e)
        _launchers = hooks
    return _launchers

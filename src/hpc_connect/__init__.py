import os
from typing import Any
from typing import Optional
from typing import Type

from .launch import HPCLauncher
from .submit import HPCProcess
from .submit import HPCScheduler
from .submit import HPCSubmissionFailedError
from .util import get_entry_points


def set(*, scheduler: Optional[str] = None, launcher: Optional[str] = None):
    if scheduler is not None:
        if backend._scheduler is None:
            for scheduler_t in schedulers():
                if scheduler_t.matches(scheduler):
                    backend.scheduler = scheduler_t()
                    break
            else:
                raise ValueError(f"No matching scheduler for {scheduler!r}")
        elif not backend._scheduler.matches(scheduler):
            raise ValueError(f"hpc_connect scheduler already set to {backend._scheduler.name}")
    if launcher is not None:
        if backend._launcher is None:
            for launcher_t in launchers():
                if launcher_t.matches(launcher):
                    backend.launcher = launcher_t()
                    break
            else:
                raise ValueError(f"No matching launcher for {launcher!r}")
        elif not backend._launcher.matches(launcher):
            raise ValueError(f"hpc_connect launcher already set to {backend._launcher.name}")


class Backend:
    def __init__(self) -> None:
        self._scheduler: Optional[HPCScheduler] = None
        self._launcher: Optional[HPCLauncher] = None

    @property
    def scheduler(self) -> HPCScheduler:
        if self._scheduler is None:
            name: str
            if var := os.getenv("HPC_CONNECT_SCHEDULER"):
                name = var
            elif var := os.getenv("HPC_CONNECT_DEFAULT_SCHEDULER"):
                name = var
            else:
                name = "default"
            for key, scheduler_t in find_schedulers().items():
                if scheduler_t.matches(name) or key == name:
                    self.scheduler = scheduler_t()
                    break
            else:
                raise ValueError(f"could not determine scheduler backend end for {name!r}")
        assert self._scheduler is not None
        return self._scheduler

    @scheduler.setter
    def scheduler(self, arg: HPCScheduler) -> None:
        assert isinstance(arg, HPCScheduler)
        self._scheduler = arg

    @property
    def launcher(self) -> HPCLauncher:
        if self._launcher is None:
            name: str
            if var := os.getenv("HPC_CONNECT_LAUNCHER"):
                name = var
            elif var := os.getenv("HPC_CONNECT_DEFAULT_LAUNCHER"):
                name = var
            else:
                name = "default"
            for key, launcher_t in find_launchers().items():
                if launcher_t.matches(name) or key == name:
                    self.launcher = launcher_t()
                    break
            else:
                raise ValueError(f"could not determine launcher backend end for {name!r}")
        assert self._launcher is not None
        return self._launcher

    @launcher.setter
    def launcher(self, arg: HPCLauncher) -> None:
        assert isinstance(arg, HPCLauncher)
        self._launcher = arg


_schedulers: Optional[dict[str, Type[HPCScheduler]]] = None
_launchers: Optional[dict[str, Type[HPCLauncher]]] = None


def find_schedulers() -> dict[str, Type[HPCScheduler]]:
    global _schedulers
    if _schedulers is None:
        from .shell_submit import ShellScheduler

        _schedulers = {"default": ShellScheduler}
        entry_points = get_entry_points(group="hpc_connect.scheduler") or []
        for entry_point in entry_points:
            _schedulers[entry_point.name] = entry_point.load()
    return _schedulers


def schedulers() -> list[Type[HPCScheduler]]:
    return list(find_schedulers().values())


def find_launchers() -> dict[str, Type[HPCLauncher]]:
    global _launchers
    if _launchers is None:
        from .mpi_launch import MPILauncher

        _launchers = {"default": MPILauncher}
        entry_points = get_entry_points(group="hpc_connect.launcher") or []
        for entry_point in entry_points:
            _launchers[entry_point.name] = entry_point.load()
    return _launchers


def launchers() -> list[Type[HPCLauncher]]:
    return list(find_launchers().values())


backend = Backend()


def __getattr__(attrname: str) -> Any:
    if attrname == "scheduler":
        return backend.scheduler
    elif attrname == "launcher":
        return backend.launcher
    else:
        raise AttributeError(attrname)

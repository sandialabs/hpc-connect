import abc
from typing import Any


class HPCProcess(abc.ABC):
    def __init__(self, args: list[str], output: str | None, error: str | None) -> None: ...

    @property
    def jobid(self) -> Any:
        return "<none>"

    @property
    @abc.abstractmethod
    def returncode(self) -> int | None: ...

    @returncode.setter
    @abc.abstractmethod
    def returncode(self, arg: int) -> None: ...

    @abc.abstractmethod
    def poll(self) -> int | None: ...

    @abc.abstractmethod
    def cancel(self) -> None: ...

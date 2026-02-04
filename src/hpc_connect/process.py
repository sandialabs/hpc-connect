import abc


class HPCProcess(abc.ABC):
    _jobid: str = "unset"
    _submitted: float = -1.0
    _started: float = -1.0

    def __init__(self, args: list[str], output: str | None, error: str | None) -> None: ...

    @property
    def submitted(self) -> float:
        return self._submitted

    @submitted.setter
    def submitted(self, arg: float) -> None:
        self._submitted = arg

    @property
    def started(self) -> float:
        return self._started

    @started.setter
    def started(self, arg: float) -> None:
        self._started = arg

    @property
    def jobid(self) -> str:
        return self._jobid

    @jobid.setter
    def jobid(self, arg: str) -> None:
        self._jobid = arg

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

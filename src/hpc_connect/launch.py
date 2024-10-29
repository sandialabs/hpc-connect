import abc
import argparse
from typing import Any
from typing import Optional


class HPCLauncher(abc.ABC):
    name = "<launcher>"

    def __init__(self) -> None:
        pass

    @staticmethod
    @abc.abstractmethod
    def matches(name: str) -> bool: ...

    @abc.abstractmethod
    def launch(self, *args_in: str, **kwargs: Any) -> int: ...

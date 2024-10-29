import abc
import argparse


class HPCLauncher(abc.ABC):
    name = "<launcher>"

    def __init__(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def executable(self) -> str: ...

    @staticmethod
    @abc.abstractmethod
    def matches(name: str) -> bool: ...

    @abc.abstractmethod
    def options(self, args: argparse.Namespace, unknown_args: list[str]) -> list[str]:
        """Return options to pass to ``self.executable``"""

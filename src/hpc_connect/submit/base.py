# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import getpass
import importlib
import os
from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Any
from typing import Protocol
from typing import TextIO

from ..config import Config
from ..util import make_template_env
from ..util import sanitize_path
from ..util import set_executable
from ..util import time_in_seconds


class HPCProcess(Protocol):
    @property
    def jobid(self) -> Any:
        return "<none>"

    @property
    def returncode(self) -> int | None:
        raise NotImplementedError

    @returncode.setter
    def returncode(self, arg: int) -> None:
        raise NotImplementedError

    def poll(self) -> int | None:
        raise NotImplementedError

    def cancel(self) -> None:
        raise NotImplementedError


class HPCSubmissionManager(ABC):
    name = "<backend>"

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()

    @staticmethod
    @abstractmethod
    def matches(name: str) -> bool: ...

    @property
    def supports_subscheduling(self) -> bool:
        return False

    def get_from_config(self, key: str, default: Any = None) -> Any:
        if value := self.config.get(f"submit:{self.name}:{key}"):
            return value
        else:
            value = self.config.get(f"submit:{key}")
            return default if value is None else value

    @property
    def default_options(self) -> list[str]:
        return self.get_from_config("default_options") or []

    @property
    def polling_frequency(self) -> float:
        s = os.getenv("HPCC_POLLING_FREQUENCY") or 30.0  # 30s.
        return time_in_seconds(s)

    def prepare_command_line(self, args: list[str]) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
        **kwargs: Any,
    ) -> HPCProcess: ...

    def submitn(
        self,
        name: list[str],
        args: list[list[str]],
        scriptname: list[str] | None = None,
        qtime: list[float] | None = None,
        submit_flags: list[list[str]] | None = None,
        variables: list[dict[str, str | None]] | None = None,
        output: list[str] | None = None,
        error: list[str] | None = None,
        nodes: list[int] | None = None,
        cpus: list[int] | None = None,
        gpus: list[int] | None = None,
        **kwargs: Any,
    ) -> HPCProcess:
        raise NotImplementedError(f"{self.name} backend has not implemented submitn")

    @property
    def submission_template(self) -> str:
        raise NotImplementedError

    def write_submission_script(
        self,
        name: str,
        args: list[str],
        scriptname: str | TextIO | None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
    ) -> str | None:
        template_dirs = {str(importlib.resources.files("hpc_connect").joinpath("templates"))}
        data = self.format_submission_data(
            name,
            args,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
        )
        template = self.submission_template
        if not os.path.exists(template):
            raise FileNotFoundError(template)
        d, f = os.path.split(os.path.abspath(template))
        template_dirs.add(d)
        env = make_template_env(*template_dirs)
        t = env.get_template(f)
        if hasattr(scriptname, "write"):
            scriptname.write(t.render(data))  # type: ignore
            return None
        scriptname = scriptname or unique_scriptname()
        file = sanitize_path(scriptname)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, "w") as fh:
            fh.write(t.render(data))
        set_executable(file)
        return file

    def format_submission_data(
        self,
        name: str,
        args: list[str],
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        #
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
    ) -> dict[str, Any]:
        if nodes is None and cpus is None:
            raise ValueError("must specify at least one of nodes and cpus")
        output = output or "stdout.txt"
        data: dict[str, Any] = {
            "name": name,
            "time": qtime or 1.0,
            "args": submit_flags or [],
            "nodes": nodes,
            "cpus": cpus,
            "gpus": gpus,
            "cpus_per_node": self.config.cpus_per_node,
            "gpus_per_node": self.config.gpus_per_node,
            "user": getpass.getuser(),
            "date": datetime.now().strftime("%c"),
            "variables": variables or {},
            "commands": args,
            "output": output,
            "error": error,
        }
        return data


def unique_scriptname() -> str:
    template = "Submit-%d.sh"
    i = 1
    while True:
        script = template % i
        if not os.path.exists(script):
            return script
        i += 1


class HPCSubmissionFailedError(Exception):
    pass

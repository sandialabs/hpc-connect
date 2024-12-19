import math
import os
import re


class Job:
    def __init__(
        self,
        *,
        name: str,
        commands: list[str],
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
        output: str | None = None,
        error: str | None = None,
        qtime: float | None = None,
        variables: dict[str, str | None] | None = None,
        script: str | None = None,
    ) -> None:
        self.name = name
        self.commands = commands

        if nodes is None and tasks is None:
            raise ValueError("one or both of nodes and tasks must be defined")
        elif nodes is not None and tasks is not None and nodes > tasks:
            raise ValueError("requesting more nodes than tasks")
        self.tasks = tasks
        self.cpus_per_task = cpus_per_task or 1
        self.gpus_per_task = gpus_per_task or 0
        self.tasks_per_node = tasks_per_node
        self.nodes = nodes
        self.output = output
        self.error = error
        self.qtime = qtime
        self.variables = variables
        self.script = sanitize_path(script or f"{self.name}-submit.sh")
        self.returncode: int | None = None

    def time_limit_in_seconds(self, pad: int = 0) -> int:
        """Return the time limit in seconds. Guarenteed return value >= 1."""
        limit = 1 if self.qtime is None else math.ceil(self.qtime)
        limit = max(limit, 1)
        limit = limit + pad if pad > 0 else limit
        return limit

    def time_limit_in_minutes(self, pad: int = 0) -> int:
        """Return the time limit in minutes. Guarenteed return value >= 1."""
        sec = self.time_limit_in_seconds()
        minutes = math.ceil(sec / 60.0)
        minutes = minutes + pad if pad > 0 else minutes
        return minutes


def sanitize_path(path: str) -> str:
    """Remove illegal file characters from ``path``"""
    dirname, basename = os.path.split(path)
    basename = re.sub(r"[^\w_. -]", "_", basename).strip("_")
    return os.path.join(dirname, basename)

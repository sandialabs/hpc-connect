import tempfile
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from pathlib import Path
from typing import Any
from typing import Mapping


@dataclass(frozen=True)
class JobSpec:
    """
    Declarative description of a single job submission.
    Scheduler- and backend-agnostic.

    Each job may have multiple sequential commands, e.g. a setup command
    followed by a compute command.
    """

    # ---- identity ----
    name: str

    # ---- execution ----
    # A list of commands
    commands: list[str]

    # ---- resources (logical request) ----
    nodes: int | None = None
    cpus: int | None = None
    gpus: int | None = None

    # ---- time ----
    time_limit: float = 1500.0

    # ---- environment ----
    env: Mapping[str, str | None] = field(default_factory=dict)

    # ---- IO ----
    # stdout/stderr for the *entire* job (all commands)
    output: str | None = None
    error: str | None = None

    workspace: Path = field(default_factory=lambda: Path(tempfile.mkdtemp()))

    submit_args: list[str] = field(default_factory=list)

    extensions: dict[str, Any] = field(default_factory=dict)

    def with_updates(self, **kwargs) -> "JobSpec":
        return replace(self, **kwargs)

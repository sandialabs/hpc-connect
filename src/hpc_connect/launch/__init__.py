import subprocess
from typing import Any
from typing import Type

from ..config import Config
from .base import HPCLauncher
from .srun import SrunLauncher

launchers: list[Type[HPCLauncher]] = [SrunLauncher, HPCLauncher]


def factory(config: Config | None = None) -> HPCLauncher:
    config = config or Config()
    exec = config.get("launch:exec")
    for launcher_t in launchers:
        if launcher_t.matches(exec):
            return launcher_t(config)
    raise ValueError(f"No matching launcher manager for {exec!r}")


def launch(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    launcher = factory()
    return launcher(args, **kwargs)

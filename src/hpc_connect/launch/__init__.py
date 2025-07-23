import subprocess
from typing import Any

from ..config import Config
from . import base
from . import srun
from .base import HPCLauncher

plugins = (base, srun)


def factory(config: Config | None = None) -> HPCLauncher:
    config = config or Config()
    launcher = config.pluginmanager.hook.hpc_connect_launcher(config=config)
    if launcher is None:
        exec = config.get("launch:exec")
        raise ValueError(f"No matching launcher manager for {exec!r}")
    return launcher


def launch(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    launcher = factory()
    return launcher(args, **kwargs)

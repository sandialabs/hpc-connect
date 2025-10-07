from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .launch import MPILauncher

if TYPE_CHECKING:
    from hpc_connect.config import Config
    from hpc_connect.launch import HPCLauncher


@hookimpl
def hpc_connect_launcher(config: "Config") -> "HPCLauncher | None":
    if MPILauncher.matches(config.get("launch:exec")):
        return MPILauncher(config=config)
    return None

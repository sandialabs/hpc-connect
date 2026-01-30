from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .backend import RemoteBackend

if TYPE_CHECKING:
    from hpc_connect.backend import Backend


@hookimpl
def hpc_connect_backend(name: str) -> "Backend | None":
    if name == "remote_subprocess":
        return RemoteBackend()
    return None

from typing import TYPE_CHECKING

from hpc_connect.hookspec import hookimpl

from .backend import LocalBackend

if TYPE_CHECKING:
    from hpc_connect.submit import HPCSubmissionManager


@hookimpl
def hpc_connect_backend(name) -> "LocalBackend | None":
    if name in ("local", "shell", "subprocess"):
        return LocalBackend()
    return None

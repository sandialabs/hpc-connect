import os
from typing import Any

from .._launch import Namespace
from ..hookspec import hookimpl


@hookimpl(trylast=True)
def hpc_connect_launch_join_args(
    args: "Namespace", exec: str, global_options: list[str], local_options: list[str]
) -> list[str]:
    cmd = [os.fsdecode(exec)]
    np = sum(p for p in args.processes if p)
    for opt in global_options:
        cmd.append(expand(opt, np=np))
    for p, spec in args:
        for opt in local_options:
            cmd.append(expand(opt, np=p))
        for arg in spec:
            cmd.append(expand(arg, np=p))
        cmd.append(":")
    return cmd[:-1]  # remove trailing ':'


def expand(arg: str, **kwargs: Any) -> str:
    return arg % kwargs

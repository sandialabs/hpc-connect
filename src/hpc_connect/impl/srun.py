import io
import os
import shutil
from typing import Any

from .._launch import Namespace
from ..hookspec import hookimpl


@hookimpl
def hpc_connect_launch_join_args(
    args: "Namespace", exec: str, default_global_options: list[str], default_local_options: list[str]
) -> list[str] | None:
    """Count the total number of processes and write a srun.conf file to
    split the jobs across ranks

    """
    srun_mpmd = exec.endswith("srun") and len(args) > 1
    if not srun_mpmd:
        return None
    np: int = 0
    fp = io.StringIO()
    for p, spec in args:
        ranks: str
        if p is not None:
            ranks = f"{np}-{np + p - 1}"
            np += p
        else:
            ranks = str(np)
            np += 1
        i = argp(spec)
        fp.write(ranks)
        for default_opt in default_local_options:
            fp.write(f" {expand(default_opt, np=np)}")
        for arg in spec[i:]:
            fp.write(f" {expand(arg, np=p)}")
        fp.write("\n")
    file = "launch-multi-prog.conf"
    with open(file, "w") as fh:
        fh.write(fp.getvalue())
    cmd = [os.fsdecode(exec)]
    for default_opt in default_global_options:
        cmd.append(expand(default_opt, np=np))
    cmd.extend([f"-n{np}", "--multi-prog", file])
    return cmd


def expand(arg: str, **kwargs: Any) -> str:
    return arg % kwargs


def argp(args: list[str]) -> int:
    for i, arg in enumerate(args):
        if shutil.which(arg):
            return i
    return -1

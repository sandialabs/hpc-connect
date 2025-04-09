# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import re
import stat
from typing import Any
from typing import Callable

from .proc import cpu_count
from .tengine import make_template_env
from .time import hhmmss
from .time import time_in_seconds


def set_executable(path: str) -> None:
    """Set executable bits on ``path``"""
    mode = os.stat(path).st_mode
    if mode & stat.S_IRUSR:
        mode |= stat.S_IXUSR
    if mode & stat.S_IRGRP:
        mode |= stat.S_IXGRP
    if mode & stat.S_IROTH:
        mode |= stat.S_IXOTH
    os.chmod(path, mode)


def partition(arg: list[Any], predicate: Callable) -> tuple[list[Any], list[Any]]:
    a: list[Any] = []
    b: list[Any] = []
    for item in arg:
        if predicate(item):
            a.append(item)
        else:
            b.append(item)
    return a, b


def sanitize_path(path: str) -> str:
    """Remove illegal file characters from ``path``"""
    dirname, basename = os.path.split(path)
    basename = re.sub(r"[^\w_. -]", "_", basename).strip("_")
    return os.path.join(dirname, basename)

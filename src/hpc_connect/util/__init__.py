import os
import re
import shutil
import stat
import subprocess
import sys

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

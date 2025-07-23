"""
Overview
--------

`hpc-launch` is a lightweight and configurable wrapper around program launchers like `mpiexec` or
`srun`. `hpc-launch` provides a single interface to multiple backends, simplifying the process of
launching applications in an HPC environment. `hpc-launch` passes all command line arguments to the
backend implementation.

Configuration
-------------

The default behavior of `hpc-launch` can be changed by providing a yaml configuration file.  The
default configuration is:

.. code-block:: yaml

   hpc_connect:
     launch:
       exec: mpiexec  # the launch backend.
       numproc_flag: -n  # Flag to pass to the backend before giving it the number of processors to run on.
       local_flags: []  # Flags to pass to the backend before any other arguments.
       default_flags: []  # Flags to pass to the backend before any other arguments.
       post_flags: []  # Flags to pass to the backend after all arguments.
       mappings: {-n: <numproc_flag>}  # Mapping of flag provided on the command line to flag passed to the backend

The configuration file is read at the first of:

1. ./hpc_connect.yaml
2. $HPCC_CONFIG_FILE
3. $XDG_CONFIG_HOME/hpc_connect/config.yaml
4. ~/.config/hpc_connect/config.yaml

Configuration settings can also be modified through the following environment variables:

* HPCC_LAUNCH_EXEC
* HPCC_LAUNCH_NUMPROC_FLAG
* HPCC_LAUNCH_LOCAL_FLAGS
* HPCC_LAUNCH_DEFAULT_FLAGS
* HPCC_LAUNCH_POST_FLAGS
* HPCC_LAUNCH_MAPPINGS

"""

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

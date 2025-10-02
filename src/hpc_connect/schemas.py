# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shlex
import shutil
from typing import Any

from schema import Optional
from schema import Or
from schema import Schema
from schema import Use

logger = logging.getLogger(__name__)


def flag_splitter(arg: list[str] | str) -> list[str]:
    if isinstance(arg, str):
        return shlex.split(arg)
    elif not isinstance(arg, list) and not all(isinstance(str, _) for _ in arg):
        raise ValueError("expected list[str]")
    return arg


def dict_str_str(arg: Any) -> bool:
    f = isinstance
    return f(arg, dict) and all([f(_, str) for k, v in arg.items() for _ in (k, v)])


class choose_from:
    def __init__(self, *choices: str | None):
        self.choices = set(choices)

    def __call__(self, arg: str | None) -> str | None:
        if arg not in self.choices:
            raise ValueError(f"Invalid choice {arg!r}, choose from {self.choices!r}")
        return arg


def which(arg: str) -> str:
    if path := shutil.which(arg):
        return path
    logger.debug(f"{arg} not found on PATH")
    return arg


# Resource spec have the following form:
# machine:
#   resources:
#   - type: node
#     count: node_count
#     resources:
#     - type: socket
#       count: sockets_per_node
#       resources:
#       - type: resource_name (like cpus)
#         count: type_per_socket
#         additional_properties:  (optional)
#         - type: slots
#           count: 1

resource_spec = {
    "type": "node",
    "count": int,
    Optional("additional_properties"): Or(dict, None),
    "resources": [
        {
            "type": str,
            "count": int,
            Optional("additional_properties"): Or(dict, None),
            Optional("resources"): [
                {
                    "type": str,
                    "count": int,
                    Optional("additional_properties"): Or(dict, None),
                },
            ],
        },
    ],
}


config_schema = Schema({Optional("debug"): bool})
launch_spec = {
    Optional("numproc_flag"): str,
    Optional("default_options"): Use(flag_splitter),
    Optional("local_options"): Use(flag_splitter),
    Optional("pre_options"): Use(flag_splitter),
    Optional("mappings"): dict_str_str,
}
launch_schema = Schema(
    {
        Optional("exec"): Use(which),
        Optional(str): launch_spec,
        **launch_spec,
    }
)
machine_schema = Schema({"resources": [resource_spec]})
submit_schema = Schema(
    {
        Optional("backend"): Use(
            choose_from(None, "shell", "slurm", "sbatch", "pbs", "qsub", "flux")
        ),
        Optional("default_options"): Use(flag_splitter),
        Optional(str): {
            Optional("default_options"): Use(flag_splitter),
        },
    },
)

hpc_connect_schema = Schema(
    {
        "hpc_connect": {
            Optional("config"): {
                Optional("debug"): bool,
            },
            Optional("submit"): {
                Optional("backend"): Use(
                    choose_from(None, "shell", "slurm", "sbatch", "pbs", "qsub", "flux")
                ),
                Optional("default_options"): Use(flag_splitter),
                Optional(str): {
                    Optional("default_options"): Use(flag_splitter),
                },
            },
            Optional("machine"): {
                Optional("resources"): Or([resource_spec], None),
            },
            Optional("launch"): {
                Optional("exec"): Use(which),
                **launch_spec,
                Optional(str): launch_spec,
            },
        }
    },
    ignore_extra_keys=True,
    description="HPC connect configuration schema",
)

resource_schema = Schema({"resources": [resource_spec]})

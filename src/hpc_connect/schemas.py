# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shlex
import shutil
import typing

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


def dict_str_str(arg: typing.Any) -> bool:
    f = isinstance
    return f(arg, dict) and all([f(_, str) for k, v in arg.items() for _ in (k, v)])


def list_of_str(arg: typing.Any) -> bool:
    return isinstance(arg, list) and all([isinstance(_, str) for _ in arg])


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


def boolean(arg: typing.Any) -> bool:
    if isinstance(arg, str):
        return arg.lower() not in ("0", "off", "false", "no")
    return bool(arg)


def load_mappings(arg: str) -> dict[str, str]:
    mappings: dict[str, str] = {}
    for kv in arg.split(","):
        k, v = [_.strip() for _ in kv.split(":") if _.split()]
        mappings[k] = v
    return mappings


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
resource_schema = Schema({"resources": [resource_spec]})
config_schema = Schema({Optional("debug"): bool, Optional("plugins"): list_of_str})
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
machine_schema = Schema({"resources": Or([resource_spec], None)})
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


class EnvarSchema(Schema):
    def validate(self, data, is_root_eval=True):
        data = super().validate(data, is_root_eval=False)
        if is_root_eval:
            final = {}
            for key, value in data.items():
                name = key[12:].lower()
                if name.startswith(("launch_", "submit_")):
                    section, _, field = name.partition("_")
                    final.setdefault(section, {})[field] = value
                else:
                    final.setdefault("config", {})[name] = value
            return final
        return data


environment_variable_schema = EnvarSchema(
    {
        Optional("HPC_CONNECT_DEBUG"): Use(boolean),
        Optional("HPC_CONNECT_PLUGINS"): Use(
            lambda x: [_.strip() for _ in x.split(",") if _.split()]
        ),
        Optional("HPC_CONNECT_LAUNCH_EXEC"): Use(which),
        Optional("HPC_CONNECT_LAUNCH_NUMPROC_FLAG"): Use(str),
        Optional("HPC_CONNECT_LAUNCH_DEFAULT_OPTIONS"): Use(flag_splitter),
        Optional("HPC_CONNECT_LAUNCH_LOCAL_OPTIONS"): Use(flag_splitter),
        Optional("HPC_CONNECT_LAUNCH_PRE_OPTIONS"): Use(flag_splitter),
        Optional("HPC_CONNECT_LAUNCH_MAPPINGS"): Use(load_mappings),
        Optional("HPC_CONNECT_SUBMIT_BACKEND"): Use(str),
        Optional("HPC_CONNECT_SUBMIT_DEFAULT_OPTIONS"): Use(flag_splitter),
    },
    ignore_extra_keys=True,
)

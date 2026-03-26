# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import copy
import shlex
from typing import Any

from schema import And
from schema import Optional as BaseOptional
from schema import Or
from schema import Schema as BaseSchema
from schema import Use


def flag_splitter(arg: list[str] | str) -> list[str]:
    if isinstance(arg, str):
        return shlex.split(arg)
    elif not isinstance(arg, list) and not all(isinstance(_, str) for _ in arg):
        raise ValueError("expected list[str]")
    return arg


class Optional(BaseOptional):
    def __init__(self, *args, default_factory=None, **kwargs):
        if default_factory is not None and kwargs.get("default"):
            raise TypeError("Mutually exclusive arguments: 'default' and 'default_factory'")
        super().__init__(*args, **kwargs)
        self.default_factory = default_factory


class Schema(BaseSchema):
    def validate(self, data: Any, *args: Any, **kwargs: Any) -> Any:
        # Apply default_factory for dict schemas before normal validation
        if isinstance(self._schema, dict) and isinstance(data, dict):
            data = dict(data)  # shallow copy so we don't mutate caller's dict
            for key, _subschema in self._schema.items():
                if isinstance(key, Optional) and (factory := getattr(key, "default_factory", None)):
                    # schema.Optional stores the raw key in .schema
                    raw_key = key.schema
                    if raw_key not in data:
                        data[raw_key] = copy.deepcopy(factory())
        return super().validate(data, *args, **kwargs)


list_of_str: And = And(lambda x: isinstance(x, list), lambda x: all(isinstance(_, str) for _ in x))
dict_str_str: And = And(
    lambda x: isinstance(x, dict),
    lambda x: all(isinstance(_, str) for k, v in x.items() for _ in (k, v)),
)


def mpmd_defaults() -> dict[str, list]:
    return {"local_options": list(), "global_options": list()}


def submit_defaults() -> dict[str, Any]:
    return {"default_options": list(), "polling_interval": -1.0}


def launch_defaults() -> dict[str, Any]:
    return {
        "type": "mpi",
        "exec": "mpiexec",
        "numproc_flag": "-n",
        "default_options": list(),
        "pre_options": list(),
        "mpmd": mpmd_defaults(),
    }


launch_schema = Schema(
    {
        "type": str,
        Optional("name"): str,
        Optional("exec"): str,
        Optional("numproc_flag", default="-n"): str,
        Optional("default_options", default_factory=list): Use(flag_splitter),
        Optional("pre_options", default_factory=list): Use(flag_splitter),
        Optional("variables", default_factory=dict): dict_str_str,
        Optional("mpmd", default_factory=mpmd_defaults): {
            Optional("local_options", default_factory=list): Use(flag_splitter),
            Optional("global_options", default_factory=list): Use(flag_splitter),
        },
    },
)

backend_schema = Schema(
    {
        "type": str,
        Optional("name"): str,
        Optional("config"): dict,
        Optional("launch"): launch_schema,
        Optional("submit"): {
            Optional("default_options", default_factory=list): list_of_str,
            Optional("polling_interval", default=-1.0): float,
        },
    }
)


config_schema = Schema(
    {
        Optional("debug", default=False): bool,
        Optional("backend"): str,
        Optional("backends", default_factory=list): [backend_schema],
    }
)


resource_node = {
    "type": str,
    "count": int,
    Optional("additional_properties"): Or(dict, None),  # type: ignore
    Optional("resources"): [Use(lambda x: x)],  # recursive schema
}
resource_schema = Schema({"resources": [resource_node]})

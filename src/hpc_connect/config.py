import copy
import logging
import os
import shlex
from contextlib import contextmanager
from typing import Any

import yaml

from .third_party.schema import Optional
from .third_party.schema import Schema
from .third_party.schema import Use
from .util import collections

logger = logging.getLogger("hpc_connect")


def flag_splitter(arg: list[str] | str) -> list[str]:
    if isinstance(arg, str):
        return shlex.split(arg)
    elif not isinstance(arg, list) and not all(isinstance(str, _) for _ in arg):
        raise ValueError("expected list[str]")
    return arg


def dict_str_str(arg: Any) -> bool:
    f = isinstance
    return f(arg, dict) and all([f(_, str) for k, v in arg.items() for _ in (k, v)])


schema = Schema(
    {
        "hpc_connect": {
            Optional("launch"): {
                Optional("vendor"): str,
                Optional("exec"): str,
                Optional("numproc_flag"): str,
                Optional("default_options"): Use(flag_splitter),
                Optional("local_options"): Use(flag_splitter),
                Optional("mappings"): dict_str_str,
            }
        }
    }
)


def load() -> dict:
    config = {
        "launch": {
            "vendor": "unknown",
            "exec": "mpiexec",
            "numproc_flag": "-n",
            "default_options": [],
            "local_options": [],
            "mappings": {},
        },
    }
    read_from_file(config)
    # Environment mods take precedence
    read_from_env(config)
    return config


def read_from_file(config: dict) -> None:
    file = find_config_file()
    if file is None:
        return
    with open(file) as fh:
        raw_config = yaml.safe_load(fh)
    fc = schema.validate(raw_config)["hpc_connect"]
    if "launch" in fc:
        for key in config["launch"]:
            if key in fc["launch"]:
                if key in ("default_options", "local_options"):
                    config["launch"][key].extend(fc["launch"][key])
                elif key == "mappings":
                    config["launch"][key].update(fc["launch"][key])
                else:
                    config["launch"][key] = fc["launch"][key]


def read_from_env(config: dict) -> None:
    for key in config["launch"]:
        var = f"HPCC_LAUNCH_{key.upper()}"
        if val := os.getenv(var):
            if key in ("default_options", "local_options"):
                if val.startswith("!"):
                    config["launch"][key] = shlex.split(val[1:])
                else:
                    config["launch"][key].extend(shlex.split(val))
            elif key == "mappings":
                if val.startswith("!"):
                    config["launch"][key] = load_mappings(val[1:])
                else:
                    config["launch"][key].update(load_mappings(val))
                config["launch"][key].update(load_mappings(val))
            else:
                config["launch"][key] = val


def load_mappings(arg: str) -> dict[str, str]:
    mappings: dict[str, str] = {}
    for kv in arg.split(","):
        k, v = [_.strip() for _ in kv.split(":") if _.split()]
        mappings[k] = v
    return mappings


def find_config_file() -> str | None:
    if os.path.exists("./hpc_connect.yaml"):
        return os.path.abspath("./hpc_connect.yaml")
    if "HPCC_CONFIG" in os.environ:
        file = os.environ["HPCC_CONFIG"]
        if os.path.exists(file):
            return file
        logger.warning(f"HPCC_CONFIG {file} does not exist")
    if "XDG_CONFIG_HOME" in os.environ:
        file = os.path.join(os.environ["XDG_CONFIG_HOME"], "hpc_connect/config.yaml")
        if os.path.exists(file):
            return file
    file = os.path.expanduser("~/.config/hpc_connect/config.yaml")
    if os.path.exists(file):
        return file
    if "HPCC_SITE_CONFIG" in os.environ:
        file = os.environ["HPCC_SITE_CONFIG"]
        if os.path.exists(file):
            return file
    return None


_config = load()


def get(path: str):
    parts = path.split(":")
    section = parts.pop(0)
    value = _config[section]
    while parts:
        key = parts.pop(0)
        # cannot use value.get(key, default) in case there is another part
        # and default is not a dict
        if key not in value:
            return None
        value = value[key]
    return value


def set(path: str, value: Any) -> None:
    parts = path.split(":")
    section = parts.pop(0)
    section_data = _config[section]
    data = section_data
    while len(parts) > 1:
        key = parts.pop(0)
        new = data[key]
        if isinstance(new, dict):
            new = dict(new)
            # reattach to parent object
            data[key] = new
        data = new
    # update new value
    data[parts[0]] = value
    _config[section] = section_data


def update(path: str, value: Any) -> None:
    """Add the given configuration to the config."""
    existing = get(path)
    if existing is None:
        new = value
    elif isinstance(existing, (dict, list)):
        new = collections.merge(existing, value)
    else:
        new = value
    set(path, new)


def restore_defaults() -> None:
    global _config
    _config = load()


@contextmanager
def override():
    global _config
    save_config = copy.deepcopy(_config)
    try:
        _config = load()
        yield
    finally:
        _config = save_config

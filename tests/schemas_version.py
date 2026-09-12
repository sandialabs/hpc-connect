# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpc_connect.schemas and hpc_connect.version."""

import pytest

from hpc_connect.schemas import Optional
from hpc_connect.schemas import Schema
from hpc_connect.schemas import backend_schema
from hpc_connect.schemas import config_schema
from hpc_connect.schemas import flag_splitter
from hpc_connect.schemas import launch_schema

# ---------------------------------------------------------------------------
# flag_splitter
# ---------------------------------------------------------------------------


def test_flag_splitter_string():
    assert flag_splitter("--ntasks 4 --mem 8G") == ["--ntasks", "4", "--mem", "8G"]


def test_flag_splitter_list_passthrough():
    lst = ["--ntasks", "4"]
    assert flag_splitter(lst) == lst


def test_flag_splitter_bad_type_raises():
    with pytest.raises(ValueError):
        flag_splitter(42)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Optional with default_factory
# ---------------------------------------------------------------------------


def test_optional_default_factory_mutually_exclusive_with_default():
    with pytest.raises(TypeError, match="Mutually exclusive"):
        Optional("key", default="x", default_factory=list)


# ---------------------------------------------------------------------------
# Schema.validate applies default_factory
# ---------------------------------------------------------------------------


def test_schema_validate_applies_default_factory():
    s = Schema({"name": str, Optional("tags", default_factory=list): list})
    result = s.validate({"name": "foo"})
    assert result["name"] == "foo"
    assert result["tags"] == []


def test_schema_validate_does_not_overwrite_provided_value():
    s = Schema({"name": str, Optional("tags", default_factory=list): list})
    result = s.validate({"name": "foo", "tags": ["a", "b"]})
    assert result["tags"] == ["a", "b"]


def test_schema_validate_default_factory_independent_instances():
    s = Schema({"key": str, Optional("items", default_factory=list): list})
    r1 = s.validate({"key": "a"})
    r2 = s.validate({"key": "b"})
    r1["items"].append(1)
    assert r2["items"] == []  # independent copies


# ---------------------------------------------------------------------------
# launch_schema
# ---------------------------------------------------------------------------


def test_launch_schema_minimal():
    result = launch_schema.validate({"type": "mpi"})
    assert result["type"] == "mpi"
    assert result["numproc_flag"] == "-n"
    assert result["default_options"] == []
    assert result["pre_options"] == []


def test_launch_schema_string_default_options_split():
    result = launch_schema.validate({"type": "mpi", "default_options": "--map-by core"})
    assert result["default_options"] == ["--map-by", "core"]


def test_launch_schema_full():
    data = {
        "type": "mpi",
        "name": "openmpi",
        "exec": "mpiexec",
        "numproc_flag": "-np",
        "default_options": ["--map-by", "core"],
        "pre_options": [],
        "mpmd": {"local_options": [], "global_options": []},
    }
    result = launch_schema.validate(data)
    assert result["numproc_flag"] == "-np"


# ---------------------------------------------------------------------------
# backend_schema
# ---------------------------------------------------------------------------


def test_backend_schema_minimal():
    result = backend_schema.validate({"type": "slurm"})
    assert result["type"] == "slurm"


def test_backend_schema_with_launch():
    data = {"type": "slurm", "name": "my.slurm", "launch": {"type": "srun"}}
    result = backend_schema.validate(data)
    assert result["name"] == "my.slurm"
    assert result["launch"]["type"] == "srun"


# ---------------------------------------------------------------------------
# config_schema
# ---------------------------------------------------------------------------


def test_config_schema_empty():
    result = config_schema.validate({})
    assert result["debug"] is False
    assert result["backends"] == []


def test_config_schema_with_backend():
    data = {"debug": True, "backend": "my.slurm", "backends": [{"type": "slurm", "name": "my.slurm"}]}
    result = config_schema.validate(data)
    assert result["debug"] is True
    assert len(result["backends"]) == 1


# ---------------------------------------------------------------------------
# hpc_connect.version
# ---------------------------------------------------------------------------


def test_version_is_string():
    from hpc_connect import version as v

    assert isinstance(v.version, str)
    assert len(v.version) > 0


def test_version_starts_with_year():
    from hpc_connect import version as v

    # date-based version: "YY.M.D[+g...]"
    major = v.version.split(".")[0]
    assert major.isdigit() or "+" in v.version


def test_version_dunder():
    from hpc_connect import version as v

    assert isinstance(v.__version__, str)
    assert v.__version__ == v.version

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
from pathlib import Path

import hpc_connect


def test_backends_returns_backend_type_strings():
    names = hpc_connect.backends()
    assert isinstance(names, list)
    assert "local" in names
    assert "slurm" in names
    assert all(isinstance(name, str) for name in names)


def test_get_backend_type_match_uses_configured_overrides(tmpdir):
    workspace = Path(tmpdir.strpath)
    cfg = workspace / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - name: named-local
      type: local
      launch:
        type: mpi
        exec: mpiexec
        numproc_flag: -n
        default_options: [--named]
      config:
        nnode: 1
        sockets_per_node: 1
        cores_per_socket: 2
    - type: local
      config:
        nnode: 3
        sockets_per_node: 2
        cores_per_socket: 5
""",
        encoding="utf-8",
    )

    old = os.environ.get("HPC_CONNECT_GLOBAL_CONFIG")
    try:
        os.environ["HPC_CONNECT_GLOBAL_CONFIG"] = str(cfg)
        hpc_connect.config.reset()
        backend = hpc_connect.get_backend("local")
    finally:
        hpc_connect.config.reset()
        if old is None:
            os.environ.pop("HPC_CONNECT_GLOBAL_CONFIG", None)
        else:
            os.environ["HPC_CONNECT_GLOBAL_CONFIG"] = old

    assert backend.node_count == 3
    assert backend.sockets_per_node == 2
    assert backend.count_per_socket("cpu") == 5


def test_get_backend_named_instance_keeps_name_specific_overrides(tmpdir):
    workspace = Path(tmpdir.strpath)
    cfg = workspace / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - name: my.local
      type: local
      config:
        nnode: 4
        sockets_per_node: 2
        cores_per_socket: 7
""",
        encoding="utf-8",
    )

    old = os.environ.get("HPC_CONNECT_GLOBAL_CONFIG")
    try:
        os.environ["HPC_CONNECT_GLOBAL_CONFIG"] = str(cfg)
        hpc_connect.config.reset()
        backend = hpc_connect.get_backend("my.local")
    finally:
        hpc_connect.config.reset()
        if old is None:
            os.environ.pop("HPC_CONNECT_GLOBAL_CONFIG", None)
        else:
            os.environ["HPC_CONNECT_GLOBAL_CONFIG"] = old

    assert backend.node_count == 4
    assert backend.sockets_per_node == 2
    assert backend.count_per_socket("cpu") == 7

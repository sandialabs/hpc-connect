# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import hpc_connect
from hpc_connect.topology import HeterogeneousTopologyError
from hpcc_flux.shell.backend import FluxAdapter as FluxShellAdapter

FluxPyAdapter = pytest.importorskip("hpcc_flux.py.backend").FluxAdapter


class FakeBackend(hpc_connect.Backend):
    type = "fake"

    def __init__(self, specs):
        self._specs = specs
        super().__init__({"type": self.type, "config": {}, "launch": {}})

    @classmethod
    def default_config(cls) -> dict:
        return {
            "type": cls.type,
            "config": {},
            "launch": {
                "default_options": [],
                "pre_options": [],
                "mpmd": {"global_options": [], "local_options": []},
            },
            "submit": {"default_options": [], "polling_interval": 1.0},
        }

    @property
    def resource_specs(self) -> list[dict]:
        return self._specs

    @property
    def valid_launchers(self) -> set[str]:
        return {"mpi"}

    def submission_manager(self):
        raise NotImplementedError

    def launch_adapter(self):
        raise NotImplementedError

    def launcher(self):
        raise NotImplementedError


def test_flux_shell_alloc_settings_raise_on_heterogeneous_nodes_for_uniform_cpu():
    backend = FakeBackend(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )
    adapter = FluxShellAdapter(
        backend=backend, config={"default_options": [], "polling_interval": 1.0}
    )

    try:
        adapter.get_alloc_settings(nodes=2)
    except HeterogeneousTopologyError:
        pass
    else:
        raise AssertionError("expected HeterogeneousTopologyError")


def test_flux_py_alloc_settings_raise_on_heterogeneous_nodes_for_uniform_cpu():
    backend = FakeBackend(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )
    adapter = FluxPyAdapter(backend=backend, config={"default_options": [], "polling_interval": 1.0})

    try:
        adapter.get_alloc_settings(nodes=2)
    except HeterogeneousTopologyError:
        pass
    else:
        raise AssertionError("expected HeterogeneousTopologyError")

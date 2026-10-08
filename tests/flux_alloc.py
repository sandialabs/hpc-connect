# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import hpc_connect
import hpc_connect.launch
from hpc_connect.topology import HeterogeneousTopologyError
from hpcc_flux.shell.backend import FluxAdapter as FluxShellAdapter
from hpcc_flux.shell.backend import FluxRunAdapter as FluxShellRunAdapter

FluxPyAdapter = pytest.importorskip("hpcc_flux.py.backend").FluxAdapter
FluxPyRunAdapter = pytest.importorskip("hpcc_flux.py.backend").FluxRunAdapter


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


def test_flux_shell_run_uses_node_level_launch_view_without_sockets(monkeypatch):
    backend = FakeBackend([{"type": "node", "count": 3, "resources": [{"type": "cpu", "count": 8}]}])
    adapter = FluxShellRunAdapter(
        backend=backend,
        config={"default_options": ["--nodes=%(nodes)d"], "pre_options": [], "mpmd": {}},
    )
    monkeypatch.setattr(adapter, "executable", lambda: ["flux", "run"])

    argv = adapter.join_specs([hpc_connect.launch.LaunchSpec(["-n", "17", "app"], processes=17)])

    assert argv == ["flux", "run", "--nodes=2", "-n", "17", "app"]


def test_flux_py_run_uses_node_level_launch_view_without_sockets(monkeypatch):
    backend = FakeBackend([{"type": "node", "count": 3, "resources": [{"type": "cpu", "count": 8}]}])
    adapter = FluxPyRunAdapter(
        backend=backend,
        config={"default_options": ["--nodes=%(nodes)d"], "pre_options": [], "mpmd": {}},
    )
    monkeypatch.setattr(adapter, "executable", lambda: ["flux", "run"])

    argv = adapter.join_specs([hpc_connect.launch.LaunchSpec(["-n", "17", "app"], processes=17)])

    assert argv == ["flux", "run", "--nodes=2", "-n", "17", "app"]

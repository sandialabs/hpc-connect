# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import hpc_connect


class FakeBackend(hpc_connect.Backend):
    type = "fake"

    def __init__(self, specs):
        self._specs = specs
        super().__init__({"type": self.type, "config": {}, "launch": {"type": "mpi"}})

    @classmethod
    def default_config(cls) -> dict:
        return {
            "type": cls.type,
            "config": {},
            "launch": {
                "type": "mpi",
                "exec": "mpiexec",
                "numproc_flag": "-n",
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

    def launcher(self):
        raise NotImplementedError


def test_count_per_node_homogeneous_topology():
    backend = FakeBackend(
        [
            {
                "type": "node",
                "count": 3,
                "resources": [
                    {"type": "socket", "count": 2, "resources": [{"type": "cpu", "count": 8}]},
                    {"type": "gpu", "count": 4},
                ],
            }
        ]
    )

    assert backend.node_count == 3
    assert backend.count_per_node("cpu") == 16
    assert backend.count_per_node("gpu") == 4
    assert backend.count_per_socket("cpu") == 8


def test_nodes_required_homogeneous_topology():
    backend = FakeBackend(
        [
            {
                "type": "node",
                "count": 4,
                "resources": [
                    {"type": "socket", "count": 2, "resources": [{"type": "cpu", "count": 8}]},
                    {"type": "gpu", "count": 2},
                ],
            }
        ]
    )

    assert backend.nodes_required(cpu=1) == 1
    assert backend.nodes_required(cpu=16) == 1
    assert backend.nodes_required(cpu=17) == 2
    assert backend.nodes_required(gpu=2) == 1
    assert backend.nodes_required(gpu=3) == 2


def test_count_per_node_heterogeneous_topology_current_behavior():
    backend = FakeBackend(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )

    # Current public behavior sums all CPU counts that roll up to a single node,
    # even for heterogeneous node groups.
    assert backend.count_per_node("cpu") == 12
    assert backend.node_count == 3


def test_resource_view_requires_socket_topology():
    backend = FakeBackend([{"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 8}]}])

    try:
        backend.resource_view(ranks=4)
    except ValueError as exc:
        assert "socket-based topology" in str(exc)
    else:
        raise AssertionError("expected ValueError for non-socket topology")

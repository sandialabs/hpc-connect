# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import hpc_connect
from hpc_connect.topology import HeterogeneousTopologyError
from hpc_connect.topology import Topology


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


def test_topology_from_resource_specs_homogeneous_entries():
    topology = Topology.from_resource_specs(
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

    assert [entry.type for entry in topology.by_type("node")] == ["node"]
    assert [entry.count for entry in topology.by_type("socket")] == [2]
    assert [entry.count for entry in topology.by_type("cpu")] == [8]
    assert [entry.count for entry in topology.by_type("gpu")] == [4]


def test_topology_from_resource_specs_heterogeneous_entries():
    topology = Topology.from_resource_specs(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )

    nodes = topology.by_type("node")
    cpus = topology.by_type("cpu")
    assert [entry.count for entry in nodes] == [2, 1]
    assert [entry.count for entry in cpus] == [4, 8]
    assert cpus[0].parent_counts == (2, 4)
    assert cpus[1].parent_counts == (1, 8)


def test_topology_uniform_per_node_homogeneous():
    topology = Topology.from_resource_specs(
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

    assert topology.is_homogeneous() is True
    assert topology.uniform_per_node("cpu") == 16
    assert topology.uniform_per_node("gpu") == 4
    assert topology.max_per_node("cpu") == 16
    assert topology.min_per_node("cpu") == 16
    assert topology.total("cpu") == 48
    assert topology.total("gpu") == 12


def test_topology_uniform_per_node_heterogeneous_raises():
    topology = Topology.from_resource_specs(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )

    assert topology.is_homogeneous() is False
    assert topology.max_per_node("cpu") == 8
    assert topology.min_per_node("cpu") == 4
    assert topology.total("cpu") == 16

    try:
        topology.uniform_per_node("cpu")
    except HeterogeneousTopologyError as exc:
        assert "not uniform" in str(exc)
    else:
        raise AssertionError("expected HeterogeneousTopologyError")


def test_topology_node_groups_returns_node_entries():
    topology = Topology.from_resource_specs(
        [
            {"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 4}]},
            {"type": "node", "count": 1, "resources": [{"type": "cpu", "count": 8}]},
        ]
    )

    groups = topology.node_groups()
    assert [group.type for group in groups] == ["node", "node"]
    assert [group.count for group in groups] == [2, 1]

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpc_connect.discover — default_resource_set."""

import json

from hpc_connect.discover import default_resource_set


def test_default_resource_set_psutil_fallback(monkeypatch):
    """Without HPC_CONNECT_HOSTFILE, should return a single-node cpu resource."""
    monkeypatch.delenv("HPC_CONNECT_HOSTFILE", raising=False)
    monkeypatch.delenv("HPC_CONNECT_HOSTNAME", raising=False)

    result = default_resource_set()

    assert isinstance(result, list)
    assert len(result) == 1
    node = result[0]
    assert node["type"] == "node"
    assert node["count"] == 1
    sockets = node["resources"]
    assert sockets[0]["type"] == "socket"
    cpus = sockets[0]["resources"]
    assert cpus[0]["type"] == "cpu"
    assert isinstance(cpus[0]["count"], int)
    assert cpus[0]["count"] > 0


def test_default_resource_set_from_hostfile(tmp_path, monkeypatch):
    """When HPC_CONNECT_HOSTFILE is set, read spec for the matching hostname."""
    hostname = "mynode01"
    spec = [{"type": "node", "count": 2, "resources": [{"type": "cpu", "count": 32}]}]
    hostfile = tmp_path / "hosts.json"
    hostfile.write_text(json.dumps({hostname: spec}))

    monkeypatch.setenv("HPC_CONNECT_HOSTFILE", str(hostfile))
    monkeypatch.setenv("HPC_CONNECT_HOSTNAME", hostname)

    result = default_resource_set()
    assert result == spec


def test_default_resource_set_hostfile_wildcard(tmp_path, monkeypatch):
    """Wildcard patterns in hostfile keys are matched via fnmatch."""
    spec = [{"type": "node", "count": 4, "resources": []}]
    hostfile = tmp_path / "hosts.json"
    hostfile.write_text(json.dumps({"mynode*": spec}))

    monkeypatch.setenv("HPC_CONNECT_HOSTFILE", str(hostfile))
    monkeypatch.setenv("HPC_CONNECT_HOSTNAME", "mynode42")

    result = default_resource_set()
    assert result == spec


def test_default_resource_set_hostfile_no_match_falls_through(tmp_path, monkeypatch):
    """If no pattern matches, fall through to the psutil default."""
    hostfile = tmp_path / "hosts.json"
    hostfile.write_text(json.dumps({"othernode": []}))

    monkeypatch.setenv("HPC_CONNECT_HOSTFILE", str(hostfile))
    monkeypatch.setenv("HPC_CONNECT_HOSTNAME", "differentnode")

    # No match → falls through to psutil path → still returns a valid list
    result = default_resource_set()
    assert result[0]["type"] == "node"

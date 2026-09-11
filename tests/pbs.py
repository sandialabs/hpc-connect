# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
from pathlib import Path

import hpcc_pbs.backend
from hpc_connect import JobSpec


def test_basic(tmpdir):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        backend = hpcc_pbs.backend.PBSBackend()
        cpus_per_node = backend.count_per_node("cpu")
        job = JobSpec(
            "my-job",
            ["ls"],
            cpus=1,
            nodes=1,
            output="my-out.txt",
            error="my-err.txt",
            workspace=Path.cwd(),
            time_limit=1.0,
            env={"MY_VAR": "SPAM"},
        )
        backend.submission_manager().adapter.submit(job)
        text = (workspace / "my-job.sh").read_text()
        assert "bin/sh" in text
        assert "#PBS -V" in text
        assert "#PBS -N my-job" in text
        assert f"#PBS -l nodes=1:ppn={cpus_per_node}" in text
        assert "#PBS -l walltime=00:00:01" in text
        assert "#PBS -o my-out.txt" in text
        assert "#PBS -e my-err.txt" in text
        assert 'export MY_VAR="SPAM"' in text
        assert "ls" in text
    finally:
        os.chdir(cwd)


def _write_spec(workspace, **kwargs):
    kwargs.setdefault("nodes", 1)
    kwargs.setdefault("workspace", workspace)
    kwargs.setdefault("time_limit", 1.0)
    name = kwargs.pop("name", "j")
    backend = hpcc_pbs.backend.PBSBackend()
    backend.submission_manager().adapter.submit(JobSpec(name, ["ls"], **kwargs))
    return (workspace / f"{name}.sh").read_text()


def _patch_gpu_count(monkeypatch, gpus_per_node, cpus_per_node=8):
    monkeypatch.setattr(
        hpcc_pbs.backend.PBSBackend,
        "count_per_node",
        lambda self, rtype, default=None: (
            gpus_per_node if rtype in ("gpu", "gpus") else cpus_per_node
        ),
    )


def test_gpu_node_requests_gpus_on_resource_line(tmpdir, monkeypatch):
    """A GPU node adds :gpus=N to the -l nodes resource line (whole-node)."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 4, cpus_per_node=8)
        text = _write_spec(workspace, name="gpu-job", cpus=8, gpus=4)
        print(text)
        assert "#PBS -l nodes=1:ppn=8:gpus=4" in text
    finally:
        os.chdir(cwd)


def test_gpus_none_on_gpu_node_reserves_whole_node(tmpdir, monkeypatch):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 2, cpus_per_node=8)
        text = _write_spec(workspace)  # gpus left as None
        assert "#PBS -l nodes=1:ppn=8:gpus=2" in text
    finally:
        os.chdir(cwd)


def test_cpu_only_node_omits_gpus(tmpdir):
    """The mock pbsnodes has no GPUs, so :gpus= is not added."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        backend = hpcc_pbs.backend.PBSBackend()
        cpus_per_node = backend.count_per_node("cpu")
        text = _write_spec(workspace, cpus=1)
        assert f"#PBS -l nodes=1:ppn={cpus_per_node}\n" in text
        assert ":gpus=" not in text
    finally:
        os.chdir(cwd)


def test_explicit_zero_gpus_omits_gpus(tmpdir, monkeypatch):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 4, cpus_per_node=8)
        text = _write_spec(workspace, cpus=8, gpus=0)
        assert ":gpus=" not in text
    finally:
        os.chdir(cwd)

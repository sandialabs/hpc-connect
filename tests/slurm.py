# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import tempfile
from pathlib import Path

import hpc_connect
import hpcc_slurm.backend
import hpcc_slurm.process


def test_basic(tmpdir):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        backend = hpcc_slurm.backend.SlurmBackend()
        spec = hpc_connect.JobSpec(
            "my-job",
            ["ls"],
            cpus=1,
            nodes=1,
            output="my-out.txt",
            error="my-err.txt",
            workspace=workspace,
            time_limit=1.0,
            env={"MY_VAR": "SPAM"},
        )
        backend.submission_manager().adapter.submit(spec)
        text = (workspace / "my-job.sh").read_text()
        print(text)
        assert "bin/sh" in text
        assert "#SBATCH --nodes=1" in text
        assert "#SBATCH --time=00:00:01" in text
        assert "#SBATCH --job-name=my-job" in text
        assert "#SBATCH --error=my-err.txt" in text
        assert "#SBATCH --output=my-out.txt" in text
        assert 'export MY_VAR="SPAM"' in text
        assert "ls" in text
    finally:
        os.chdir(cwd)


def _patch_gpu_count(monkeypatch, gpus_per_node, cpus_per_node=48):
    monkeypatch.setattr(
        hpcc_slurm.backend.SlurmBackend,
        "count_per_node",
        lambda self, rtype, default=None: (
            gpus_per_node if rtype in ("gpu", "gpus") else cpus_per_node
        ),
    )


def _write_spec(workspace, **kwargs):
    kwargs.setdefault("nodes", 1)
    kwargs.setdefault("workspace", workspace)
    kwargs.setdefault("time_limit", 1.0)
    name = kwargs.pop("name", "j")
    spec = hpc_connect.JobSpec(name, ["ls"], **kwargs)
    backend = hpcc_slurm.backend.SlurmBackend()
    backend.submission_manager().adapter.submit(spec)
    return (workspace / f"{name}.sh").read_text()


def test_gpu_node_requests_gres(tmpdir, monkeypatch):
    """A GPU node reserves all its GPUs via --gres (whole-node model)."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 4)
        text = _write_spec(workspace, name="gpu-job", cpus=4, gpus=4)
        print(text)
        assert "#SBATCH --gres=gpu:4" in text
    finally:
        os.chdir(cwd)


def test_gpus_none_on_gpu_node_reserves_whole_node(tmpdir, monkeypatch):
    """gpus=None (canary's default) still reserves the node's GPUs."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 2)
        text = _write_spec(workspace)  # gpus left as None
        assert "#SBATCH --gres=gpu:2" in text
    finally:
        os.chdir(cwd)


def test_positive_gpu_request_reserves_whole_node(tmpdir, monkeypatch):
    """Even a single-GPU job reserves the whole node's GPUs (whole-node model)."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 8)
        text = _write_spec(workspace, cpus=1, gpus=1)
        assert "#SBATCH --gres=gpu:8" in text
    finally:
        os.chdir(cwd)


def test_cpu_only_node_emits_no_gres(tmpdir):
    """The mock sinfo has no GRES gpu, so no --gres directive is written."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        text = _write_spec(workspace, cpus=1)
        assert "--gres" not in text
    finally:
        os.chdir(cwd)


def test_explicit_zero_gpus_emits_no_gres(tmpdir, monkeypatch):
    """gpus=0 means a CPU job even on a GPU-capable node: no --gres."""
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    cwd = Path.cwd()
    try:
        os.chdir(workspace)
        _patch_gpu_count(monkeypatch, 4)
        text = _write_spec(workspace, cpus=4, gpus=0)
        assert "--gres" not in text
    finally:
        os.chdir(cwd)


def test_parse_script_args():
    with tempfile.NamedTemporaryFile("w") as fh:
        fh.write("""\
#!/bin/sh
#SBATCH --nodes=1
#SBATCH --time=00:00:01
#SBATCH --job-name=my-job
#SBATCH --error=my-err.txt
#SBATCH --output=my-out.txt
#SBATCH --clusters=flight,eclipse
export MY_VAR=SPAM
printenv || true
ls""")
        fh.seek(0)
        ns = hpcc_slurm.process.parse_script_args(fh.name)
        assert ns.clusters == "flight,eclipse"

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

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import io
import os
from contextlib import contextmanager

import hpc_connect


@contextmanager
def tmp_environ():
    save_env = os.environ.copy()
    try:
        os.environ["HPC_CONNECT_CPUS_PER_NODE"] = "10"
        os.environ["HPC_CONNECT_GPUS_PER_NODE"] = "0"
        os.environ["HPC_CONNECT_NODE_COUNT"] = "1"
        yield
    finally:
        os.environ.clear()
        os.environ.update(save_env)


def test_basic():
    backend = hpc_connect.submit.pbs.PBSSubmissionManager()
    with io.StringIO() as fh:
        backend.write_submission_script(
            "my-job",
            ["ls"],
            fh,
            cpus=1,
            nodes=1,
            output="my-out.txt",
            error="my-err.txt",
            qtime=1.0,
            variables={"MY_VAR": "SPAM"},
        )
        text = fh.getvalue()
    assert "#!/bin/sh" in text
    assert "#PBS -N my-job" in text
    assert f"#PBS -l nodes=1:ppn={backend.config.cpus_per_node}" in text
    assert "#PBS -l walltime=00:00:01" in text
    assert "#PBS --job-name=my-job" in text
    assert "#PBS -o my-out.txt" in text
    assert "#PBS -e my-err.txt" in text
    assert "export MY_VAR=SPAM" in text
    assert "printenv || true" in text
    assert "ls" in text

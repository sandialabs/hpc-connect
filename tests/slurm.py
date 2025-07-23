# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import io
import os
import tempfile
from contextlib import contextmanager

import hpc_connect


@contextmanager
def tmp_environ():
    save_env = os.environ.copy()
    try:
        os.environ["SLURM_NTASKS_PER_NODE"] = "10"
        os.environ["SLURM_NNODES"] = "1"
        os.environ["SLURM_GPUS"] = "0"
        yield
    finally:
        os.environ.clear()
        os.environ.update(save_env)


def test_basic():
    backend = hpc_connect.submit.slurm.SlurmSubmissionManager()
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
    assert "#SBATCH --nodes=1" in text
    assert "#SBATCH --time=00:00:01" in text
    assert "#SBATCH --job-name=my-job" in text
    assert "#SBATCH --error=my-err.txt" in text
    assert "#SBATCH --output=my-out.txt" in text
    assert "export MY_VAR=SPAM" in text
    assert "printenv || true" in text
    assert "ls" in text


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
        ns = hpc_connect.submit.slurm.SlurmProcess.parse_script_args(fh.name)
        assert ns.clusters == "flight,eclipse"

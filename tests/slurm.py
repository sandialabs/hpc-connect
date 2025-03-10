import io
import os
from contextlib import contextmanager

import hpc_connect
import hpc_connect.job

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
    slurm = hpc_connect.impl.slurm.SlurmScheduler()
    job = hpc_connect.job.Job(
        name="my-job",
        commands=["ls"],
        tasks=1,
        cpus_per_task=1,
        gpus_per_task=0,
        tasks_per_node=10,
        nodes=1,
        output="my-out.txt",
        error="my-err.txt",
        qtime=1.0,
        variables={"MY_VAR": "SPAM"},
    )
    with io.StringIO() as fh:
        slurm.write_submission_script(job, fh)
        text = fh.getvalue()
    assert "#!/bin/sh" in text
    assert "#SBATCH --nodes=1" in text
    assert "#SBATCH --ntasks-per-node=10" in text
    assert "#SBATCH --cpus-per-task=1" in text
    assert "#SBATCH --time=00:00:01" in text
    assert "#SBATCH --job-name=my-job" in text
    assert "#SBATCH --error=my-err.txt" in text
    assert "#SBATCH --output=my-out.txt" in text
    assert "export MY_VAR=SPAM" in text
    assert "printenv || true" in text
    assert "ls" in text

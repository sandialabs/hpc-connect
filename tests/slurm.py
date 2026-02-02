# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import hpc_connect
import hpcc_slurm.backend
import hpcc_slurm.process


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


def test_basic(tmpdir):
    dir = Path(tmpdir.strpath)
    dir.mkdir(exist_ok=True)
    config = hpc_connect.Config.from_defaults(overrides=dict(backend="slurm"))
    backend = hpcc_slurm.backend.SlurmBackend(config=config)
    spec = hpc_connect.JobSpec(
        "my-job",
        ["ls"],
        cpus=1,
        nodes=1,
        workspace=dir,
        output="my-out.txt",
        error="my-err.txt",
        time_limit=1.0,
        env={"MY_VAR": "SPAM"},
    )
    backend.submission_manager().adapter.submit(spec)
    text = (dir / "my-job.sh").read_text()
    assert "#!/bin/sh" in text
    assert "#SBATCH --nodes=1" in text
    assert "#SBATCH --time=00:00:01" in text
    assert "#SBATCH --job-name=my-job" in text
    assert "#SBATCH --error=my-err.txt" in text
    assert "#SBATCH --output=my-out.txt" in text
    assert 'export MY_VAR="SPAM"' in text
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
        ns = hpcc_slurm.process.SlurmProcess.parse_script_args(fh.name)
        assert ns.clusters == "flight,eclipse"

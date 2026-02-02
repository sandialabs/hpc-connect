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
        assert "#!/bin/sh" in text
        assert "#SBATCH --nodes=1" in text
        assert "#SBATCH --time=00:00:01" in text
        assert "#SBATCH --job-name=my-job" in text
        assert "#SBATCH --error=my-err.txt" in text
        assert "#SBATCH --output=my-out.txt" in text
        assert 'export MY_VAR="SPAM"' in text
        assert "ls" in text
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
        ns = hpcc_slurm.process.SlurmProcess.parse_script_args(fh.name)
        assert ns.clusters == "flight,eclipse"

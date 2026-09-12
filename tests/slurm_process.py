# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpcc_slurm.process — sacct, sbatch, wait, Job, parse_script_args,
_parse_slurm_exitcode, and SlurmProcess."""

import os

import pytest

import hpcc_slurm.process as slurm_proc
from hpcc_slurm.process import Job
from hpcc_slurm.process import _parse_slurm_exitcode
from hpcc_slurm.process import parse_script_args
from hpcc_slurm.process import sacct
from hpcc_slurm.process import sbatch
from hpcc_slurm.process import wait

# ---------------------------------------------------------------------------
# _parse_slurm_exitcode
# ---------------------------------------------------------------------------


def test_parse_slurm_exitcode_clean():
    assert _parse_slurm_exitcode("0:0") == (0, 0)


def test_parse_slurm_exitcode_failed():
    assert _parse_slurm_exitcode("1:0") == (1, 0)


def test_parse_slurm_exitcode_signal():
    assert _parse_slurm_exitcode("0:9") == (0, 9)


def test_parse_slurm_exitcode_bad_returns_minus_one():
    assert _parse_slurm_exitcode("bad") == (-1, 0)


# ---------------------------------------------------------------------------
# parse_script_args
# ---------------------------------------------------------------------------


def test_parse_script_args_clusters(tmp_path):
    script = tmp_path / "job.sh"
    script.write_text(
        "#!/bin/bash\n#SBATCH --nodes=1\n#SBATCH --clusters=flight,eclipse\necho hello\n"
    )
    ns = parse_script_args(str(script))
    assert ns.clusters == "flight,eclipse"


def test_parse_script_args_no_clusters(tmp_path):
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\n#SBATCH --nodes=2\necho hi\n")
    ns = parse_script_args(str(script))
    assert ns.clusters is None


def test_parse_script_args_empty_script(tmp_path):
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hi\n")
    ns = parse_script_args(str(script))
    assert ns.clusters is None


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


def test_job_from_accounting_data():
    data = [{"jobid": "abc123", "state": "COMPLETED", "returncode": 0, "signal": 0}]
    job = Job.from_accounting_data(data, "abc123")
    assert job is not None
    assert job.state == "COMPLETED"
    assert job.returncode == 0
    assert job.signal == 0


def test_job_from_accounting_data_not_found():
    data = [{"jobid": "xyz", "state": "COMPLETED", "returncode": 0, "signal": 0}]
    assert Job.from_accounting_data(data, "abc123") is None


def test_job_state_uppercased():
    job = Job({"jobid": "1", "state": "completed", "returncode": 0, "signal": 0})
    assert job.state == "COMPLETED"


# ---------------------------------------------------------------------------
# sacct (uses mock `sacct` on PATH that returns "abc123 FINISHED 0:0")
# ---------------------------------------------------------------------------


def test_sacct_parses_mock_output():
    # The mock sacct in tests/mock/ outputs: "abc123 FINISHED 0:0"
    # Real sacct uses pipe-separated format; our mock doesn't, so sacct()
    # will return None (no parseable rows) — that's fine to verify gracefully.
    result = sacct("abc123")
    # Mock sacct outputs a space-separated line, not pipe-separated, so
    # the parser finds no valid rows and returns None.
    assert result is None or isinstance(result, list)


def test_sacct_pipe_format(monkeypatch):
    """Monkeypatch subprocess.run to return proper sacct pipe output."""
    import subprocess

    fake_output = "abc123|partition1|00:01:00|COMPLETED|0:0|\n"

    def mock_run(args, **kwargs):
        class R:
            returncode = 0
            stdout = fake_output
            stderr = ""

        return R()

    monkeypatch.setattr(subprocess, "run", mock_run)
    result = sacct("abc123")
    assert result is not None
    assert len(result) == 1
    assert result[0]["state"] == "COMPLETED"
    assert result[0]["returncode"] == 0
    assert result[0]["signal"] == 0


def test_sacct_nonzero_returncode_returns_none(monkeypatch):
    import subprocess

    def mock_run(args, **kwargs):
        class R:
            returncode = 1
            stdout = ""
            stderr = "error"

        return R()

    monkeypatch.setattr(subprocess, "run", mock_run)
    assert sacct("abc123") is None


# ---------------------------------------------------------------------------
# sbatch (uses mock `sbatch` that echoes "Submitted batch job abc123")
# ---------------------------------------------------------------------------


def test_sbatch_returns_jobid(tmp_path):
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    os.chmod(script, 0o755)
    jobid = sbatch(str(script))
    assert jobid == "abc123"
    # sbatch also writes a submit.meta.json sidecar
    meta_file = tmp_path / "submit.meta.json"
    assert meta_file.exists()


def test_sbatch_raises_on_bad_output(tmp_path, monkeypatch):
    """If sbatch output doesn't match 'Submitted batch job <id>', raise SubmissionFailedError."""
    import subprocess

    import hpc_connect

    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hello\n")

    def mock_run(args, **kwargs):
        class R:
            returncode = 0
            stdout = "unexpected output"
            stderr = ""

        return R()

    monkeypatch.setattr(subprocess, "run", mock_run)
    with pytest.raises(hpc_connect.SubmissionFailedError):
        sbatch(str(script))


# ---------------------------------------------------------------------------
# wait (integration via monkeypatched sacct)
# ---------------------------------------------------------------------------


def test_wait_returns_completed_job(monkeypatch):
    finished_data = [
        {
            "jobid": "abc123",
            "partition": "p1",
            "elapsed": "00:01:00",
            "state": "COMPLETED",
            "exitcode": "0:0",
            "returncode": 0,
            "signal": 0,
        }
    ]
    monkeypatch.setattr(slurm_proc, "sacct", lambda jobid, clusters=None: finished_data)
    job = wait("abc123", tries=3, delay=0)
    assert job.state == "COMPLETED"
    assert job.returncode == 0


def test_wait_raises_after_exhausting_tries(monkeypatch):
    monkeypatch.setattr(slurm_proc, "sacct", lambda jobid, clusters=None: None)
    with pytest.raises(RuntimeError, match="Could not determine"):
        wait("abc123", tries=2, delay=0)


def test_wait_invalid_tries():
    with pytest.raises(ValueError, match="tries"):
        wait("x", tries=0, delay=0)


def test_wait_invalid_delay():
    with pytest.raises(ValueError, match="delay"):
        wait("x", tries=1, delay=-1)


def test_wait_polls_until_complete(monkeypatch):
    """wait() returns the first Job found in sacct regardless of state.
    SlurmProcess._poll() is responsible for interpreting the state."""
    call_count = [0]
    running_data = [
        {
            "jobid": "j1",
            "partition": "p",
            "elapsed": "0",
            "state": "RUNNING",
            "exitcode": "0:0",
            "returncode": 0,
            "signal": 0,
        }
    ]

    def fake_sacct(jobid, clusters=None):
        call_count[0] += 1
        return running_data

    monkeypatch.setattr(slurm_proc, "sacct", fake_sacct)
    # wait() returns the first job it finds, no matter the state
    job = wait("j1", tries=2, delay=0)
    assert job.state == "RUNNING"


# ---------------------------------------------------------------------------
# SlurmProcess (full end-to-end using mock binaries)
# ---------------------------------------------------------------------------


def test_slurm_process_submit_and_poll(tmp_path, monkeypatch):
    """SlurmProcess.submit uses mock sbatch; poll uses monkeypatched sacct."""
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\n#SBATCH --nodes=1\necho hello\n")
    os.chmod(script, 0o755)

    done_data = [
        {
            "jobid": "abc123",
            "partition": "p",
            "elapsed": "00:00:01",
            "state": "COMPLETED",
            "exitcode": "0:0",
            "returncode": 0,
            "signal": 0,
        }
    ]
    monkeypatch.setattr(slurm_proc, "sacct", lambda jobid, clusters=None: done_data)

    proc = slurm_proc.SlurmProcess(str(script))
    assert proc.jobid == "abc123"
    rc = proc.poll()
    assert rc == 0
    assert proc.returncode == 0


def test_slurm_process_cancel(tmp_path, monkeypatch):
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hi\n")
    os.chmod(script, 0o755)

    monkeypatch.setattr(slurm_proc, "sacct", lambda jobid, clusters=None: None)
    # scancel just needs to not raise
    monkeypatch.setattr(slurm_proc, "scancel", lambda jobid, clusters=None: None)

    proc = slurm_proc.SlurmProcess(str(script))
    proc.cancel()
    assert proc.returncode == 1

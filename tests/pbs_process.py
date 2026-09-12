# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""Tests for hpcc_pbs.process — PBSProcess.poll, cancel, and submit."""

import json
import os

import pytest

import hpc_connect
from hpcc_pbs.process import PBSProcess

# ---------------------------------------------------------------------------
# PBSProcess.submit (via mock qsub that outputs "123abc")
# ---------------------------------------------------------------------------


def test_pbs_process_submit(tmp_path):
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\n#PBS -N testjob\necho hi\n")
    os.chmod(script, 0o755)

    proc = PBSProcess(str(script))
    assert proc.jobid == "123abc"
    meta = tmp_path / "qsub.meta.json"
    assert meta.exists()
    data = json.loads(meta.read_text())
    assert "meta" in data


def test_pbs_process_submit_bad_output_raises(tmp_path, monkeypatch):
    """If qsub output is not a single token, raise SubmissionFailedError."""
    import subprocess

    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hi\n")

    def mock_popen(args, **kwargs):
        class FakePopen:
            def communicate(self):
                return b"Error: queue full\nPlease try again", None

        return FakePopen()

    monkeypatch.setattr(subprocess, "Popen", mock_popen)
    with pytest.raises(hpc_connect.SubmissionFailedError):
        PBSProcess(str(script))


# ---------------------------------------------------------------------------
# PBSProcess.poll (monkeypatched qstat output)
# ---------------------------------------------------------------------------


def _make_proc(tmp_path, monkeypatch):
    """Create a PBSProcess without actually calling qsub."""
    script = tmp_path / "job.sh"
    script.write_text("#!/bin/bash\necho hi\n")
    os.chmod(script, 0o755)
    return PBSProcess(str(script))


def test_pbs_process_poll_running(tmp_path, monkeypatch):
    """qstat output contains our jobid → job still running → None returned."""
    import subprocess

    proc = _make_proc(tmp_path, monkeypatch)
    jobid = proc.jobid  # "123abc" from mock qsub

    qstat_output = (
        "Job id            Name             User              Time Use S Queue\n"
        "----------------  ---------------- ----------------  -------- - -----\n"
        f"{jobid}           testjob          user                     0 R serial\n"
    )

    def mock_check_output(args, **kwargs):
        return qstat_output

    monkeypatch.setattr(subprocess, "check_output", mock_check_output)
    result = proc.poll()
    assert result is None


def test_pbs_process_poll_completed(tmp_path, monkeypatch):
    """qstat output does NOT contain our jobid → job finished → returncode 0."""
    import subprocess

    proc = _make_proc(tmp_path, monkeypatch)

    def mock_check_output(args, **kwargs):
        return ""  # empty — job not in qstat → completed

    monkeypatch.setattr(subprocess, "check_output", mock_check_output)
    result = proc.poll()
    assert result == 0
    assert proc.returncode == 0


def test_pbs_process_poll_truncated_jobid(tmp_path, monkeypatch):
    """qstat may truncate long jobids with a trailing '*'."""
    import subprocess

    proc = _make_proc(tmp_path, monkeypatch)
    # Mock a truncated match: jobid is "123abc", qstat shows "123a*"
    qstat_output = (
        "Job id            Name             User              Time Use S Queue\n"
        "----------------  ---------------- ----------------  -------- - -----\n"
        "123a*             testjob          user                     0 R serial\n"
    )

    def mock_check_output(args, **kwargs):
        return qstat_output

    monkeypatch.setattr(subprocess, "check_output", mock_check_output)
    result = proc.poll()
    assert result is None  # still running via truncated match


# ---------------------------------------------------------------------------
# PBSProcess.cancel
# ---------------------------------------------------------------------------


def test_pbs_process_cancel(tmp_path, monkeypatch):

    proc = _make_proc(tmp_path, monkeypatch)
    # cancel calls qdel (mock is on PATH); check returncode is set to 1
    proc.cancel()
    assert proc.returncode == 1

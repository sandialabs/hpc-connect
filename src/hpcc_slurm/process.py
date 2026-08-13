# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import argparse
import datetime
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import time
from typing import Any

import hpc_connect

logger = logging.getLogger("hpc_connect.slurm.submit")


class SlurmProcess(hpc_connect.HPCProcess):
    def __init__(self, script: str, emit_interval: float = 300.0) -> None:
        self._rc: int | None = None
        self.clusters: str | None = None
        self.script = os.path.abspath(script)
        self.script_dir = os.path.dirname(self.script)
        self.jobid = self.submit(self.script)
        self.last_debug_emit = -1.0
        self.emit_interval = emit_interval
        f = os.path.basename(self.script)
        logger.debug(f"Submitted batch script {f} with jobid={self.jobid}")

    def submit(self, script: str) -> str:
        ns = parse_script_args(script)
        if ns.clusters:
            self.clusters = ns.clusters
        jobid = sbatch(script)
        self.submitted = time.time()
        return jobid

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        try:
            return self._poll()
        except:
            logger.exception("Failed to poll")
            raise

    def _poll(self) -> int | None:
        job = wait(self.jobid, clusters=self.clusters)
        if job.state == "RUNNING" and self.started <= 0.0:
            self.started = time.time()
        if job.state in {"PENDING", "RUNNING"}:
            return None
        self.returncode = max(job.returncode, job.signal)
        self.completion_info = job.data
        if job.signal:
            logger.error("Job %s failed with signal %s", self.jobid, job.signal)
        return self.returncode

    def cancel(self) -> None:
        logger.warning(f"cancelling slurm job {self.jobid}")
        scancel(self.jobid, clusters=self.clusters)
        data = sacct(self.jobid, clusters=self.clusters)
        if data is not None:
            job = Job.from_accounting_data(data, self.jobid)
            if job is not None:
                self.completion_info = job.data
        self.returncode = 1


def which(name: str) -> str:
    path: str | None = shutil.which(name)
    if path is None:
        raise RuntimeError(f"{name} not found on PATH")
    assert isinstance(path, str)
    return path


def scancel(jobid: str, clusters: str | None = None) -> None:
    args = [which("scancel"), jobid]
    if clusters is not None:
        args.append(f"--clusters={clusters}")
    subprocess.run(args, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def sacct(jobid: str, clusters: str | None = None) -> dict[str, Any] | None:
    args = [which("sacct"), "-j", jobid, "--json"]
    if clusters is not None:
        args.append(f"--clusters={clusters}")
    cp = subprocess.run(args, text=True, capture_output=True, check=False)
    if cp.returncode != 0:
        logger.warning("%s: returned non-zero status %s", shlex.join(args), cp.returncode)
        return None
    out = cp.stdout.strip()
    if not out:
        return None
    try:
        data = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid sacct JSON from {shlex.join(args)}:\n{out}") from exc
    if "jobs" not in data:
        raise RuntimeError(f"missing 'jobs' key in sacct output from {shlex.join(args)}")
    return data


def sbatch(script: str) -> str:
    args: list[str] = [which("sbatch"), script]
    cp = subprocess.run(args, check=True, text=True, capture_output=True)
    script_dir = os.path.dirname(script)
    with open(os.path.join(script_dir, "submit.meta.json"), "w") as fh:
        date = datetime.datetime.now().strftime("%c")
        meta = {"args": " ".join(args), "date": date, "stdout": cp.stdout, "stderr": cp.stderr}
        json.dump({"meta": meta}, fh, indent=2)
    if match := re.match(r"Submitted batch job (\S*)", cp.stdout):
        jobid = match.group(1).strip()
        return jobid
    logger.error("Failed to find jobid!\n    The following output was received from sbatch:")
    for line in cp.stdout.split("\n"):
        logger.log(logging.ERROR, f"    {line}")
    for line in cp.stderr.split("\n"):
        logger.log(logging.ERROR, f"    {line}")
    raise hpc_connect.SubmissionFailedError


def parse_script_args(script: str) -> argparse.Namespace:
    args: list[str] = []
    with open(script, "r") as file:
        for line in file:
            if match := re.search(r"^#SBATCH\s+(.*)$", line):
                args.extend(shlex.split(match.group(1).strip()))
    p = argparse.ArgumentParser()
    p.add_argument("-M", "--cluster", "--clusters", dest="clusters")
    ns, _ = p.parse_known_args(args)
    return ns


def wait(jobid: str, clusters: str | None = None, tries: int = 20, delay: float = 0.5) -> "Job":
    for _ in range(tries):
        data = sacct(jobid, clusters=clusters)
        if data is not None:
            job = Job.from_accounting_data(data, jobid)
            if job is not None:
                return job
        time.sleep(delay)
    raise RuntimeError(f"Could not determine state from sacct for job {jobid}")


class Job:
    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data

    @classmethod
    def from_accounting_data(cls, data: dict[str, Any], jobid: str) -> "Job | None":
        for job in data["jobs"]:
            if str(job["job_id"]) == jobid:
                return Job(job)
        return None

    @property
    def state(self) -> str:
        return self.data["state"]["current"][0].upper()

    @property
    def returncode(self) -> int:
        exit_code = self.data["exit_code"]
        return exit_code["return_code"]["number"]

    @property
    def signal(self) -> int:
        exit_code = self.data["exit_code"]
        return exit_code["signal"]["id"]["number"]

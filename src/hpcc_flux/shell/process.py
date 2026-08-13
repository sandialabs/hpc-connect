# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import datetime
import json
import logging
import os
import shutil
import subprocess
import time

import hpc_connect

logger = logging.getLogger("hpc_connect.flux.shell.process")


class FluxProcess(hpc_connect.HPCProcess):
    def __init__(self, script: str, emit_interval: float = 300.0) -> None:
        self._rc: int | None = None
        self.script = os.path.abspath(script)
        self.script_dir = os.path.dirname(self.script)
        self.jobid = self.submit(script)
        self.last_debug_emit = -1.0
        self.emit_interval = emit_interval
        f = os.path.basename(self.script)
        logger.debug(f"Submitted batch script {f} with jobid={self.jobid}")

    def submit(self, script: str) -> str:
        flux = shutil.which("flux")
        if flux is None:
            raise ValueError("flux not found on PATH")
        job_name = os.path.basename(os.path.splitext(script)[0])
        args = [flux, "batch", "--job-name", job_name, script]
        proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        self.submitted = time.time()
        with open(os.path.join(self.script_dir, "submit.meta.json"), "w") as fh:
            date = datetime.datetime.now().strftime("%c")
            meta = {"args": " ".join(args), "date": date, "stdout/stderr": proc.stdout}
            json.dump({"meta": meta}, fh, indent=2)
        if jobid := proc.stdout.strip():
            return jobid
        logger.error(
            f"Failed to find flux jobid!\n    The following output was received from {flux} batch:"
        )
        for line in proc.stdout.split("\n"):
            logger.log(logging.ERROR, f"    {line}")
        for line in proc.stderr.split("\n"):
            logger.log(logging.ERROR, f"    {line}")
        raise hpc_connect.SubmissionFailedError

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        flux = shutil.which("flux")
        if flux is None:
            raise RuntimeError("flux not found on PATH")
        args = [flux, "jobs", "--json", self.jobid]
        cp = subprocess.run(args, text=True, check=False, capture_output=True)
        if cp.returncode != 0:
            raise RuntimeError(f"flux job {self.jobid} not found")
        info = json.loads(cp.stdout)
        if info["state"] == "RUN" and self.started <= 0.0:
            self.started = time.time()
        if info["state"] in ("DEPEND", "PRIORITY", "SCHED", "RUN", "CLEANUP"):
            return None
        self.returncode = info["returncode"]
        self.completion_info = info
        return self.returncode

    def cancel(self) -> None:
        flux = shutil.which("flux")
        if flux is None:
            raise RuntimeError("flux not found on PATH")
        logger.warning(f"cancelling flux job {self.jobid}")
        subprocess.run([flux, "cancel", self.jobid])
        self.returncode = 1

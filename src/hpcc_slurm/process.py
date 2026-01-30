# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import argparse
import datetime
import json
import logging
import os
import re
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
        self._jobid = self.submit(script)
        self.last_debug_emit = -1.0
        self.emit_interval = emit_interval
        f = os.path.basename(self.script)
        logger.debug(f"Submitted batch script {f} with jobid={self.jobid}")

    @property
    def jobid(self) -> str:
        return self._jobid

    def submit(self, script: str) -> str:
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        ns = self.parse_script_args(script)
        if ns.clusters:
            self.clusters = ns.clusters
        args = [sbatch, script]
        proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        with open(os.path.join(self.script_dir, "submit.meta.json"), "w") as fh:
            date = datetime.datetime.now().strftime("%c")
            meta = {"args": " ".join(args), "date": date, "stdout/stderr": proc.stdout}
            json.dump({"meta": meta}, fh, indent=2)
        if match := re.match(r"Submitted batch job (\S*)", proc.stdout):
            jobid = match.group(1).strip()
            return jobid
        logger.error(f"Failed to find jobid!\n    The following output was received from {sbatch}:")
        for line in proc.stdout.split("\n"):
            logger.log(logging.ERROR, f"    {line}")
        for line in proc.stderr.split("\n"):
            logger.log(logging.ERROR, f"    {line}")
        raise SubmissionFailedError

    @staticmethod
    def parse_script_args(script: str) -> argparse.Namespace:
        args = []
        with open(script, "r") as file:
            for line in file:
                if match := re.search(r"^#SBATCH\s+(.*)$", line):
                    args.append(match.group(1).strip())
        p = argparse.ArgumentParser()
        p.add_argument("-M", "--cluster", "--clusters", dest="clusters")
        ns, _ = p.parse_known_args(args)
        return ns

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        sacct = shutil.which("sacct")
        if sacct is None:
            raise RuntimeError("sacct not found on PATH")
        max_tries: int = 20
        acct_data: dict[str, dict[str, Any]] = {}
        for _ in range(max_tries):
            args = [sacct, "--noheader", "-j", self.jobid, "-p", "-b"]
            if self.clusters:
                args.append(f"--clusters={self.clusters}")
            proc = subprocess.run(args, encoding="utf-8", capture_output=True)
            out = proc.stdout
            lines = [line.strip() for line in out.splitlines() if line.split()]
            now = time.time()
            if now - self.last_debug_emit >= self.emit_interval:
                logger.debug(f"Polling slurm job {self.jobid}:\n$ {' '.join(args)!r}\n{out}")
                self.last_debug_emit = now
            if proc.returncode != 0:
                logger.warning(f"sacct returned non-zero status {proc.returncode}")
                continue
            if lines:
                for line in lines:
                    jobid, state, exit_code = [_.strip() for _ in line.split("|") if _.split()]
                    try:
                        returncode, signal = [int(_) for _ in exit_code.split(":")]
                    except ValueError:
                        returncode = int(exit_code)
                        signal = 0
                    acct_data[jobid] = {
                        "state": state.split()[0].rstrip("+"),
                        "returncode": returncode,
                        "signal": signal,
                    }
                break
            time.sleep(0.5)
        else:
            cmd, err = " ".join(args), proc.stderr or ""
            raise RuntimeError(
                f"$ {cmd}\n{out}\n{err}\n==> Error: could not determine state from accounting data"
            )

        if jobinfo := acct_data.get(self.jobid):
            if jobinfo["state"].upper() in ("PENDING", "RUNNING"):
                return None
            self.returncode = max(jobinfo["returncode"], jobinfo["signal"])
            if jobinfo["signal"]:
                logger.error(f"Job {self.jobid} failed with signal {jobinfo['signal']}")
                f = os.path.join(self.script_dir, f"{self.jobid}.acct.json")
                with open(f, "w") as fh:
                    args = [sacct, "-j", self.jobid, "--json"]
                    subprocess.run(args, stdout=fh, encoding="utf-8")
        else:
            raise RuntimeError(f"Accounting data for job {self.jobid} not returned by sacct")
        return self.returncode

    def cancel(self) -> None:
        logger.warning(f"cancelling slurm job {self.jobid}")
        subprocess.run(["scancel", self.jobid, "--clusters=all"])
        self.returncode = 1


class SubmissionFailedError(Exception):
    pass

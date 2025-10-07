# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import argparse
import datetime
import importlib.resources
import json
import logging
import os
import re
import shutil
import subprocess
import time
from typing import Any

from hpc_connect.config import Config
from hpc_connect.config import ConfigScope
from hpc_connect.submit import HPCProcess
from hpc_connect.submit import HPCSubmissionFailedError
from hpc_connect.submit import HPCSubmissionManager

from .discover import read_sinfo

logger = logging.getLogger(__name__)


class SlurmProcess(HPCProcess):
    def __init__(self, script: str) -> None:
        self._rc: int | None = None
        self.clusters: str | None = None
        self.script = os.path.abspath(script)
        self.script_dir = os.path.dirname(self.script)
        self._jobid = self.submit(script)
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
        raise HPCSubmissionFailedError

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
            if proc.returncode != 0:
                logger.warning(f"sacct returned non-zero status {proc.returncode}")
                continue
            out = proc.stdout
            lines = [line.strip() for line in out.splitlines() if line.split()]
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


class SlurmSubmissionManager(HPCSubmissionManager):
    """Setup and submit jobs to the slurm scheduler"""

    name = "slurm"

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() in ("slurm", "sbatch")

    def __init__(self, config: Config | None = None) -> None:
        super().__init__(config=config)
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        sacct = shutil.which("sacct")
        if sacct is None:
            raise ValueError("sacct not found on PATH")
        if self.config.get("machine:resources") is None:
            if sinfo := read_sinfo():
                scope = ConfigScope("slurm", None, {"machine": {"resources": [sinfo]}})
                self.config.push_scope(scope)
            else:
                logger.warning("Unable to determine system configuration from sinfo, using default")

    @property
    def submission_template(self) -> str:
        if "HPCC_SLURM_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_SLURM_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpcc_slurm").joinpath("templates/submit.sh.in"))

    def prepare_command_line(self, args: list[str]) -> list[str]:
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        return [sbatch, *self.default_options, *args]

    def submit(
        self,
        name: str,
        args: list[str],
        scriptname: str | None = None,
        qtime: float | None = None,
        submit_flags: list[str] | None = None,
        variables: dict[str, str | None] | None = None,
        output: str | None = None,
        error: str | None = None,
        nodes: int | None = None,
        cpus: int | None = None,
        gpus: int | None = None,
        **kwargs: Any,
    ) -> SlurmProcess:
        cpus = cpus or kwargs.get("tasks")  # backward compatible
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            nodes=nodes,
            cpus=cpus,
            gpus=gpus,
        )
        assert script is not None
        return SlurmProcess(script)

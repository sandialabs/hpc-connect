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

from ..hookspec import hookimpl
from ..types import HPCBackend
from ..types import HPCProcess
from ..types import HPCSubmissionFailedError

logger = logging.getLogger("hpc_connect")


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
        if match := re.search("Submitted batch job (.*)$", proc.stdout):
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
                if match := re.search("^#SBATCH\s+(.*)$", line):
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
            proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
            lines = [line.strip() for line in proc.stdout.splitlines() if line.split()]
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
            raise RuntimeError(f"{' '.join(args)!r} did not return any accounting data")

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


class SlurmBackend(HPCBackend):
    """Setup and submit jobs to the slurm scheduler"""

    name = "slurm"

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() in ("slurm", "sbatch")

    def __init__(self) -> None:
        super().__init__()
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        sacct = shutil.which("sacct")
        if sacct is None:
            raise ValueError("sacct not found on PATH")
        if sinfo := read_einfo():
            self.config.set_resource_spec([sinfo])
        elif sinfo := read_sinfo():
            self.config.set_resource_spec([sinfo])
        else:
            logger.warning("Unable to determine system configuration from sinfo, using default")

    @property
    def submission_template(self) -> str:
        if "HPCC_SLURM_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_SLURM_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpc_connect").joinpath("templates/slurm.sh.in"))

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


def read_sinfo() -> dict[str, Any] | None:
    if sinfo := shutil.which("sinfo"):
        opts = [
            "%X",  # Number of sockets per node
            "%Y",  # Number of cores per socket
            "%Z",  # Number of threads per core
            "%c",  # Number of CPUs per node
            "%D",  # Number of nodes
            "%G",  # General resources
        ]
        format = " ".join(opts)
        args = [sinfo, "-o", format]
        try:
            proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        except subprocess.CalledProcessError:
            return None
        else:
            for line in proc.stdout.split("\n"):
                parts = line.split()
                if not parts:
                    continue
                elif parts and parts[0].startswith("SOCKETS"):
                    continue
                spn, cps, _, cpn, nc, *gres = [safe_loads(_) for _ in parts]
                break
            else:
                raise ValueError(f"Unable to read sinfo output:\n{proc.stdout}")
            info = {"name": "node", "type": None, "count": nc}
            resources = info.setdefault("resources", [])
            resources.append({"name": "socket", "type": None, "count": spn})
            resources.append({"name": "cpu", "type": None, "count": cps * spn})
            for res in gres:
                if not res:
                    continue
                parts = res.split(":")
                resource: dict[str, Any] = {
                    "name": parts[0],
                    "type": None,
                    "count": safe_loads(parts[-1]),
                }
                if len(parts) > 2:
                    resource["type"] = ":".join(parts[1:-1])
                resources.append(resource)
            return info
    return None


def read_einfo() -> dict[str, Any] | None:
    """Read information from slurm allocation environment"""
    if "SLURM_JOBID" not in os.environ:
        return None
    node_count = safe_loads(os.environ["SLURM_JOB_NUM_NODES"])
    info = {"name": "node", "type": None, "count": node_count}
    resources = info.setdefault("resources", [])

    cpus_per_node = safe_loads(os.environ["SLURM_CPUS_ON_NODE"])
    resources.append({"name": "cpu", "type": None, "count": cpus_per_node})

    gpus_per_node = safe_loads(os.getenv("SLURM_GPUS_ON_NODE", "null")) or 0
    resources.append({"name": "gpu", "type": None, "count": gpus_per_node})

    return info


def safe_loads(arg: str) -> Any:
    if arg == "(null)":
        return None
    if arg.endswith("+"):
        return safe_loads(arg[:-1])
    try:
        return json.loads(arg)
    except json.JSONDecodeError:
        return arg


@hookimpl
def hpc_connect_backend():
    return SlurmBackend

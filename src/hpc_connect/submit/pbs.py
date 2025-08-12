# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import datetime
import importlib.resources
import json
import logging
import os
import shutil
import subprocess
from typing import Any

from ..config import Config
from ..hookspec import hookimpl
from .base import HPCProcess
from .base import HPCSubmissionFailedError
from .base import HPCSubmissionManager

logger = logging.getLogger(__name__)


class PBSProcess(HPCProcess):
    def __init__(self, script: str) -> None:
        self._rc: int | None = None
        self._jobid = self.submit(script)
        logger.debug(f"Submitted batch with jobid={self.jobid}")

    def submit(self, script: str) -> str:
        qsub = shutil.which("qsub")
        if qsub is None:
            raise RuntimeError("qsub not found on PATH")
        args = [qsub, script]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, _ = p.communicate()
        result = str(out.decode("utf-8")).strip()
        dirname, basename = os.path.split(script)
        with open(os.path.join(dirname, "qsub.meta.json"), "w") as fh:
            date = datetime.datetime.now().strftime("%c")
            meta = {"args": " ".join(args), "date": date, "stdout/stderr": result}
            json.dump({"meta": meta}, fh, indent=2)
        parts = result.split()
        if len(parts) == 1 and parts[0]:
            return parts[0]
        logger.error("Failed to find jobid!")
        logger.error(f"    The following output was received from {qsub}:")
        for line in result.split("\n"):
            logger.error(f"    {line}")
        raise HPCSubmissionFailedError

    @property
    def jobid(self) -> str:
        return self._jobid

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

    def poll(self) -> int | None:
        qstat = shutil.which("qstat")
        if qstat is None:
            raise RuntimeError("qstat not found on PATH")
        out = subprocess.check_output([qstat], encoding="utf-8")
        lines = [line.strip() for line in out.splitlines() if line.split()]
        for line in lines:
            # Output of qstat is something like:
            # Job id            Name             User              Time Use S Queue
            # ----------------  ---------------- ----------------  -------- - -----
            # 9932285.string-*  spam.sh          username                 0 W serial
            parts = line.split()
            if len(parts) >= 6:
                jid, state = parts[0], parts[4]
                if jid == self.jobid:
                    # Job is still running
                    return None
                elif jid[-1] == "*" and self.jobid.startswith(jid[:-1]):
                    # the output from qstat may return a truncated job id,
                    # so match the beginning of the incoming 'jobids' strings
                    return None
        # Job not found in qstat, assume it completed
        self.returncode = 0
        return self.returncode

    def cancel(self) -> None:
        logger.warning(f"cancelling pbs job {self.jobid}")
        qdel = shutil.which("qdel")
        if qdel is None:
            raise RuntimeError("qdel not found on PATH")
        self.returncode = 1


class PBSSubmissionManager(HPCSubmissionManager):
    """Setup and submit jobs to the PBS scheduler"""

    name = "pbs"

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() in ("pbs", "qsub")

    def __init__(self, config: Config | None = None) -> None:
        super().__init__(config=config)
        qsub = shutil.which("qsub")
        if qsub is None:
            raise ValueError("qsub not found on PATH")
        qstat = shutil.which("qstat")
        if qstat is None:
            raise ValueError("qstat not found on PATH")
        qdel = shutil.which("qdel")
        if qdel is None:
            raise ValueError("qdel not found on PATH")

    @property
    def submission_template(self) -> str:
        if "HPCC_PBS_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_PBS_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpc_connect").joinpath("templates/pbs.sh.in"))

    def prepare_command_line(self, args: list[str]) -> list[str]:
        qsub = shutil.which("qsub")
        if qsub is None:
            raise ValueError("qsub not found on PATH")
        return [qsub, *self.default_options, *args]

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
    ) -> PBSProcess:
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
        return PBSProcess(script)


@hookimpl
def hpc_connect_submission_manager(config) -> HPCSubmissionManager | None:
    if PBSSubmissionManager.matches(config.get("submit:backend")):
        return PBSSubmissionManager(config=config)
    return None

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

logger = logging.getLogger("hpc_connect.pbs.submit")


class PBSProcess(hpc_connect.HPCProcess):
    def __init__(self, script: str) -> None:
        self._rc: int | None = None
        self.jobid = self.submit(script)
        logger.debug(f"Submitted batch with jobid={self.jobid}")

    def submit(self, script: str) -> str:
        qsub = shutil.which("qsub")
        if qsub is None:
            raise RuntimeError("qsub not found on PATH")
        args = [qsub, script]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, _ = p.communicate()
        result = str(out.decode("utf-8")).strip()
        self.submitted = time.time()
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
        logger.error(f"    qsub submission line: {' '.join(args)}")
        with open(script) as fh:
            script_lines = fh.read()
        logger.error(f"    qsub script: {script_lines}")
        raise SubmissionFailedError

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
                    if self.started <= 0.0:
                        self.started = time.time()
                    return None
                elif jid[-1] == "*" and self.jobid.startswith(jid[:-1]):
                    # the output from qstat may return a truncated job id,
                    # so match the beginning of the incoming 'jobids' strings
                    if self.started <= 0.0:
                        self.started = time.time()
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


class SubmissionFailedError(Exception):
    pass

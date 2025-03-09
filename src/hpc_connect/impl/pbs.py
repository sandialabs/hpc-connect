import importlib.resources
import logging
import os
import shutil
import subprocess

from ..hookspec import hookimpl
from ..submit import HPCProcess
from ..submit import HPCScheduler
from ..submit import HPCSubmissionFailedError
from ..submit import Job
from ..util import set_executable

logger = logging.getLogger("hpc_connect")


class PBSProcess(HPCProcess):
    def __init__(self, job: Job) -> None:
        super().__init__(job=job)
        qsub = shutil.which("qsub")
        if qsub is None:
            raise RuntimeError("qsub not found on PATH")
        args = [qsub, job.script]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, _ = p.communicate()
        result = str(out.decode("utf-8")).strip()
        parts = result.split()
        if len(parts) == 1 and parts[0]:
            jobid = parts[0]
        else:
            logger.error("Failed to find jobid!")
            logger.error(f"    The following output was received from {qsub}:")
            for line in result.split("\n"):
                logger.error(f"    {line}")
            raise HPCSubmissionFailedError
        self.jobid = jobid

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

    def cancel(self, returncode: int) -> None:
        logger.warning(f"cancelling pbs job {self.jobid}")
        qdel = shutil.which("qdel")
        if qdel is None:
            raise RuntimeError("qdel not found on PATH")
        self.returncode = returncode


class PBSScheduler(HPCScheduler):
    """Setup and submit jobs to the PBS scheduler"""

    name = "pbs"
    shell = "/bin/sh"

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() in ("pbs", "qsub")

    @property
    def submission_template(self) -> str:
        if "HPCC_PBS_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_PBS_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpcc_pbs").joinpath("templates/pbs.sh.in"))

    def submit(self, job: Job) -> HPCProcess:
        os.makedirs(os.path.dirname(job.script), exist_ok=True)
        with open(job.script, "w") as fh:
            self.write_submission_script(job, fh)
        set_executable(job.script)
        return PBSProcess(job)


@hookimpl
def hpc_connect_scheduler():
    return PBSScheduler

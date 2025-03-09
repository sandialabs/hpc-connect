import importlib.resources
import logging
import math
import os
import shutil
import subprocess

from ..hookspec import hookimpl
from ..launch import HPCLauncher
from ..submit import HPCProcess
from ..submit import HPCScheduler
from ..submit import HPCSubmissionFailedError
from ..submit import Job
from ..util import set_executable

logger = logging.getLogger("hpc_connect")


class SlurmLauncher(HPCLauncher):
    def __init__(self, *hints: str, config_file: str | None = None) -> None:
        hints = hints or ("srun",)
        for hint in hints:
            exe = shutil.which(hint)
            if exe is not None:
                break
        else:
            raise ValueError(f"{hints[0]} not found on PATH")
        assert exe is not None
        self._executable = os.fsdecode(exe)

    @classmethod
    def factory(self, arg: str, config_file: str | None = None) -> "SlurmLauncher | None":
        if arg == "slurm":
            return SlurmLauncher()
        elif os.path.basename(arg) == "srun":
            return SlurmLauncher(arg)
        return None

    @property
    def executable(self) -> str:
        return self._executable


class SlurmProcess(HPCProcess):
    def __init__(self, job: Job) -> None:
        super().__init__(job=job)
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        args = [sbatch, job.script]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, _ = p.communicate()
        result = str(out.decode("utf-8")).strip()
        i = result.find("Submitted batch job")
        if i >= 0:
            parts = result[i:].split()
            if len(parts) > 3 and parts[3]:
                jobid = parts[3]
        else:
            logger.error("Failed to find jobid!")
            logger.error(f"    The following output was received from {sbatch}:")
            for line in result.split("\n"):
                logger.log(logging.ERROR, f"    {line}")
            raise HPCSubmissionFailedError
        logger.debug(f"Submitted batch with jobid={jobid}")
        self.jobid = jobid

    def poll(self) -> int | None:
        squeue = shutil.which("squeue")
        if squeue is None:
            raise RuntimeError("queue not found on PATH")
        out = subprocess.check_output([squeue, "--noheader", "-o", "%i %t"], encoding="utf-8")
        lines = [line.strip() for line in out.splitlines() if line.split()]
        for line in lines:
            # a line should be something like "16004759 PD"
            try:
                id, state = line.split()
            except ValueError:
                continue
            if id == self.jobid:
                # the job is still running
                return None
        # job not found in squeue, assume it is done
        self.returncode = 0
        return self.returncode

    def cancel(self, returncode: int) -> None:
        subprocess.run(["scancel", self.jobid, "--cluster=all"])
        self.returncode = returncode


class SlurmScheduler(HPCScheduler):
    """Setup and submit jobs to the slurm scheduler"""

    name = "slurm"
    command_name = "sbatch"

    def __init__(self) -> None:
        super().__init__()
        self.variables["HPC_CONNECT_DEFAULT_LAUNCHER"] = "srun"
        if val := os.getenv("SLURM_NTASKS_PER_NODE"):
            # must be on an allocated node
            self.config.cpus_per_node = int(val)  # assumes 1 task per cpu
            self.config.node_count = int(os.environ["SLURM_NNODES"])
            gpu_count = int(os.environ.get("SLURM_GPUS", 0))
            self.config.gpus_per_node = math.ceil(gpu_count / self.config.node_count)
        elif sinfo := read_sinfo():
            cores_per_socket: int = sinfo["cores_per_socket"]
            gpus_per_socket: int = sinfo.get("gpus_per_socket", 0)
            sockets_per_node: int = sinfo["sockets_per_node"]
            if "HPC_CONNNECT_CPUS_PER_NODE" not in os.environ:
                self.config.cpus_per_node = cores_per_socket * sockets_per_node
            if "HPC_CONNNECT_GPUS_PER_NODE" not in os.environ:
                self.config.gpus_per_node = gpus_per_socket * sockets_per_node
            if "HPC_CONNECT_NODE_COUNT" not in os.environ:
                self.config.node_count = sinfo["node_count"]
        else:
            logger.warning("Unable to determine system configuration from sinfo, using default")

    @staticmethod
    def matches(name: str | None) -> bool:
        return name is not None and name.lower() in ("slurm", SlurmScheduler.command_name)

    @property
    def submission_template(self) -> str:
        if "HPCC_SLURM_SUBMIT_TEMPLATE" in os.environ:
            return os.environ["HPCC_SLURM_SUBMIT_TEMPLATE"]
        return str(importlib.resources.files("hpcc_slurm").joinpath("templates/slurm.sh.in"))

    def submit(self, job: Job) -> HPCProcess:
        os.makedirs(os.path.dirname(job.script), exist_ok=True)
        with open(job.script, "w") as fh:
            self.write_submission_script(job, fh)
        set_executable(job.script)
        return SlurmProcess(job)


def read_sinfo() -> dict[str, int] | None:
    if sinfo := shutil.which("sinfo"):
        opts = [
            "%X",  # Number of sockets per node
            "%Y",  # Number of cores per socket
            "%Z",  # Number of threads per core
            "%c",  # Number of CPUs per node
            "%D",  # Number of nodes
        ]
        format = " ".join(opts)
        args = [sinfo, "-o", format]
        try:
            output = subprocess.check_output(args, encoding="utf-8")
        except subprocess.CalledProcessError:
            return None
        else:
            for line in output.split("\n"):
                parts = line.split()
                if not parts:
                    continue
                elif parts and parts[0].startswith("SOCKETS"):
                    continue
                spn, cps, _, cpn, nc = [integer(_) for _ in parts]
                break
            else:
                raise ValueError(f"Unable to read sinfo output:\n{output}")
            info = {
                "sockets_per_node": spn,
                "cores_per_socket": cps,
                "cpu_count": spn * cps * nc,
                "node_count": nc,
            }
            return info
    return None


def integer(arg: str) -> int:
    if arg.endswith("+"):
        return int(arg[:-1])
    return int(arg)


@hookimpl
def hpc_connect_scheduler():
    return SlurmScheduler


@hookimpl
def hpc_connect_launcher():
    return SlurmLauncher

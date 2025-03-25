import importlib.resources
import logging
import math
import os
import shutil
import subprocess

from ..hookspec import hookimpl
from ..types import HPCBackend
from ..types import HPCProcess
from ..types import HPCSubmissionFailedError

logger = logging.getLogger("hpc_connect")


class SlurmProcess(HPCProcess):
    def __init__(self, script: str) -> None:
        self._rc: int | None = None
        self.jobid = self.submit(script)
        f = os.path.basename(script)
        logger.info(f"Submitted batch script {f} with jobid={self.jobid}")

    def submit(self, script) -> str:
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        args = [sbatch, script]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        out, _ = p.communicate()
        result = str(out.decode("utf-8")).strip()
        i = result.find("Submitted batch job")
        if i >= 0:
            parts = result[i:].split()
            if len(parts) > 3 and parts[3]:
                return parts[3]
        logger.error("Failed to find jobid!\n    The following output was received from {sbatch}:")
        for line in result.split("\n"):
            logger.log(logging.ERROR, f"    {line}")
        raise HPCSubmissionFailedError

    @property
    def returncode(self) -> int | None:
        return self._rc

    @returncode.setter
    def returncode(self, arg: int) -> None:
        self._rc = arg

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

    def cancel(self) -> None:
        logger.info(f"cancelling batch {self.jobid}")
        subprocess.run(["scancel", self.jobid, "--cluster=all"])
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
        squeue = shutil.which("squeue")
        if squeue is None:
            raise ValueError("queue not found on PATH")
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
        #
        tasks: int | None = None,
        cpus_per_task: int | None = None,
        gpus_per_task: int | None = None,
        tasks_per_node: int | None = None,
        nodes: int | None = None,
    ) -> SlurmProcess:
        script = self.write_submission_script(
            name,
            args,
            scriptname,
            qtime=qtime,
            submit_flags=submit_flags,
            variables=variables,
            output=output,
            error=error,
            tasks=tasks,
            cpus_per_task=cpus_per_task,
            gpus_per_task=gpus_per_task,
            tasks_per_node=tasks_per_node,
            nodes=nodes,
        )
        assert script is not None
        return SlurmProcess(script)


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
def hpc_connect_backend():
    return SlurmBackend

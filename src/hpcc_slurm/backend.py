# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
import shutil

import hpc_connect
from hpc_connect.mpi import MPIExecAdapter
from hpc_connect.util import set_executable
from hpc_connect.util.time import hhmmss

from .discover import read_sinfo
from .launch import SrunAdapter
from .process import SlurmProcess

logger = logging.getLogger("hpc_connect.slurm.submit")


class SlurmBackend(hpc_connect.Backend):
    name = "slurm"

    def __init__(self, config: hpc_connect.Config | None = None) -> None:
        super().__init__(config=config)
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        sacct = shutil.which("sacct")
        if sacct is None:
            raise ValueError("sacct not found on PATH")
        self._resource_specs: list[dict]
        if sinfo := read_sinfo():
            self._resource_specs = [sinfo]
        else:
            raise ValueError("Unable to determine system configuration from sinfo")

    @property
    def resource_specs(self) -> list[dict]:
        return self._resource_specs

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        config = self.config.submit.resolve("slurm")
        return hpc_connect.HPCSubmissionManager(adapter=SbatchAdapter(config=config))

    def launcher(self) -> hpc_connect.HPCLauncher:
        name = os.path.basename(self.config.launch.exec)
        if name == "srun":
            config = self.config.launch.resolve("srun")
            return hpc_connect.HPCLauncher(adapter=SrunAdapter(backend=self, config=config))
        if name in ("mpiexec", "mpirun"):
            config = self.config.launch.resolve("mpiexec")
            return hpc_connect.HPCLauncher(adapter=MPIExecAdapter(backend=self, config=config))
        raise ValueError(f"{name}: unknown launcher for slurm backend")


class SbatchAdapter:
    def __init__(self, config: hpc_connect.SubmitConfig):
        self.config = config
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")

    def polling_interval(self) -> float:
        return self.config.polling_interval or 5.0

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            fh.write(f"#SBATCH --nodes={spec.nodes}\n")
            fh.write(f"#SBATCH --time={hhmmss(spec.time_limit * 1.25, threshold=0)}\n")
            fh.write(f"#SBATCH --job-name={spec.name}\n")
            if spec.error:
                fh.write(f"#SBATCH --error={spec.error}\n")
            if spec.output:
                fh.write(f"#SBATCH --output={spec.output}\n")
            for arg in self.config.default_options:
                fh.write(f"#SBATCH {arg}\n")
            for arg in spec.submit_args:
                fh.write(f"#SBATCH {arg}\n")
            for var, val in spec.env.items():
                if val is None:
                    fh.write(f"unset {var}\n")
                else:
                    fh.write(f'export {var}="{val}"\n')
            for command in spec.commands:
                fh.write(f"{command}\n")
        set_executable(script)
        return spec.with_updates(commands=[str(script)])

    def submit(self, spec: hpc_connect.JobSpec, exclusive: bool = True) -> hpc_connect.HPCProcess:
        s = self.prepare(spec)
        return SlurmProcess(s.commands[0])

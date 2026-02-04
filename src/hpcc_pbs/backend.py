# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shutil

import hpc_connect
from hpc_connect.mpi import MPIExecAdapter
from hpc_connect.util import set_executable
from hpc_connect.util.time import hhmmss

from .discover import read_pbsnodes
from .process import PBSProcess

logger = logging.getLogger("hpc_connect.pbs.backend")


class PBSBackend(hpc_connect.Backend):
    name = "pbs"

    def __init__(self, config: hpc_connect.Config | None = None) -> None:
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
        self._resource_specs: list[dict]
        if resources := read_pbsnodes():
            self._resource_specs = resources
        else:
            raise ValueError("Unable to determine system configuration from pbsnodes")

    @property
    def resource_specs(self) -> list[dict]:
        return self._resource_specs

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        config = self.config.submit.resolve("pbs")
        return hpc_connect.HPCSubmissionManager(adapter=QsubAdapter(backend=self, config=config))

    def launcher(self) -> hpc_connect.HPCLauncher:
        return hpc_connect.HPCLauncher(
            adapter=MPIExecAdapter(backend=self, config=self.config.launch.resolve("mpiexec"))
        )


class QsubAdapter:
    def __init__(self, backend: PBSBackend, config: hpc_connect.SubmitConfig) -> None:
        qsub = shutil.which("qsub")
        if qsub is None:
            raise ValueError("qsub not found on PATH")
        self.config = config
        self.backend = backend

    def polling_interval(self) -> float:
        return self.config.polling_interval or 5.0

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        cpus_per_node = self.backend.count_per_node("cpu")
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            fh.write("#PBS -V\n")
            fh.write(f"#PBS -N {spec.name}\n")
            fh.write(f"#PBS -l nodes={spec.nodes}:ppn={cpus_per_node}\n")
            fh.write(f"#PBS -l walltime={hhmmss(spec.time_limit * 1.25, threshold=0)}\n")
            if spec.output:
                if spec.output == spec.error:
                    fh.write("#PBS -j oe\n")
                fh.write(f"#PBS -o {spec.output}\n")
            if spec.error:
                if spec.error != spec.output:
                    fh.write(f"#PBS -e {spec.error}\n")
            for arg in self.config.default_options:
                fh.write(f"#PBS {arg}\n")
            for arg in spec.submit_args:
                fh.write(f"#PBS {arg}\n")
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
        return PBSProcess(s.commands[0])

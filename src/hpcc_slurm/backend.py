# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shutil
from typing import Any

import hpc_connect
from hpc_connect.mpi import MPIExecAdapter
from hpc_connect.util import set_executable
from hpc_connect.util.time import hhmmss

from .discover import read_sinfo
from .launch import SrunAdapter
from .process import SlurmProcess

logger = logging.getLogger("hpc_connect.slurm.submit")


class SlurmBackend(hpc_connect.Backend):
    type = "slurm"

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")
        sacct = shutil.which("sacct")
        if sacct is None:
            raise ValueError("sacct not found on PATH")
        self._resource_specs: list[dict] | None = None
        super().__init__(cfg=cfg)

    @property
    def resource_specs(self) -> list[dict]:
        if self._resource_specs is None:
            if sinfo := read_sinfo():
                self._resource_specs = [sinfo]
            else:
                raise ValueError("Unable to determine system configuration from sinfo")
        assert self._resource_specs is not None
        return self._resource_specs

    @property
    def valid_launchers(self) -> set[str]:
        return {"srun", "mpi"}

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return {
            "config": {},
            "type": cls.type,
            "launch": {
                "type": "srun",
                "exec": "srun",
                "numproc_flag": "-n",
                "default_options": [],
                "pre_options": [],
                "mpmd": {"global_options": [], "local_options": []},
            },
            "submit": {"default_options": [], "polling_interval": 15.0},
        }

    def supports_dependencies(self) -> bool:
        return True

    def submission_manager(self) -> hpc_connect.HPCSubmissionManager:
        return hpc_connect.HPCSubmissionManager(
            adapter=SbatchAdapter(backend=self, config=self.config["submit"])
        )

    def launcher(self) -> hpc_connect.HPCLauncher:
        type = self.config["launch"]["type"]
        if type == "srun":
            return hpc_connect.HPCLauncher(
                adapter=SrunAdapter(backend=self, config=self.config["launch"])
            )
        else:
            return hpc_connect.HPCLauncher(
                adapter=MPIExecAdapter(backend=self, config=self.config["launch"])
            )


class SbatchAdapter:
    def __init__(self, backend: SlurmBackend, config: dict[str, Any]) -> None:
        self.config = config
        self.backend = backend
        sbatch = shutil.which("sbatch")
        if sbatch is None:
            raise ValueError("sbatch not found on PATH")

    def polling_interval(self) -> float:
        if self.config["polling_interval"] > 0:
            return self.config["polling_interval"]
        return 15.0

    def gpus_per_node(self, spec: hpc_connect.JobSpec) -> int:
        """Number of GPUs to request on each node of the allocation.

        canary reserves whole nodes, so if the job requests any GPUs at all we
        request every GPU the node exposes (discovered from ``sinfo`` GRES).
        Returns 0 when the node has no GPUs or the job requested none, in which
        case no ``--gres`` directive is emitted (keeps CPU-only Slurm targets
        unchanged).
        """
        per_node = self.backend.count_per_node("gpu", default=0)
        if per_node <= 0:
            return 0
        # gpus is None => resource request unknown; assume a whole-node GPU job
        # (this is how the flux backend behaves).  gpus == 0 => explicitly no
        # GPUs.  Any positive request => reserve the node's GPUs.
        if spec.gpus is None or spec.gpus > 0:
            return per_node
        return 0

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        gpus_per_node = self.gpus_per_node(spec)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            fh.write(f"#SBATCH --nodes={spec.nodes}\n")
            fh.write(f"#SBATCH --time={hhmmss(spec.time_limit * 1.25, threshold=0)}\n")
            fh.write(f"#SBATCH --job-name={spec.name}\n")
            if gpus_per_node > 0:
                fh.write(f"#SBATCH --gres=gpu:{gpus_per_node}\n")
            if spec.error:
                fh.write(f"#SBATCH --error={spec.error}\n")
            if spec.output:
                fh.write(f"#SBATCH --output={spec.output}\n")
            if spec.dependencies:
                fh.write(f"#SBATCH --dependency=afterany:{':'.join(spec.dependencies)}\n")
            for arg in self.config["default_options"]:
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

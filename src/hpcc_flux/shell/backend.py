# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import math
import shutil
from typing import Any

import hpc_connect
from hpc_connect.mpi import MPIExecAdapter
from hpc_connect.util import set_executable

from ..discover import read_resource_info
from .process import FluxProcess

logger = logging.getLogger("hpc_connect.flux.shell.backend")


class FluxBackend(hpc_connect.Backend):
    type = "flux.sh"

    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        flux = shutil.which("flux")
        if flux is None:
            raise ValueError("flux not found on PATH")
        self._resource_specs: list[dict] | None = None
        super().__init__(cfg=cfg)

    @classmethod
    def matches(cls, arg: str) -> bool:
        return arg in ("flux-shell", "flux.sh", "flux:sh")

    @property
    def resource_specs(self) -> list[dict]:
        if self._resource_specs is None:
            if info := read_resource_info():
                self._resource_specs = [info]
            else:
                raise ValueError("Unable to determine system configuration from flux")
        assert self._resource_specs is not None
        return self._resource_specs

    @property
    def valid_launchers(self) -> set[str]:
        return {"mpi"}

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
            adapter=FluxAdapter(backend=self, config=self.config["submit"])
        )

    def launcher(self) -> hpc_connect.HPCLauncher:
        return hpc_connect.HPCLauncher(
            adapter=MPIExecAdapter(backend=self, config=self.config["launch"])
        )


class FluxAdapter:
    def __init__(self, backend: FluxBackend, config: dict[str, Any]) -> None:
        self.config = config
        self.backend = backend
        flux = shutil.which("flux")
        if flux is None:
            raise ValueError("flux not found on PATH")

    def polling_interval(self) -> float:
        if self.config["polling_interval"] > 0:
            return self.config["polling_interval"]
        return 15.0

    def prepare(self, spec: hpc_connect.JobSpec) -> hpc_connect.JobSpec:
        duration = int(spec.time_limit + 60)
        sh = shutil.which("sh")
        script = spec.workspace / f"{spec.name}.sh"
        script.parent.mkdir(exist_ok=True)
        alloc = self.get_alloc_settings(spec.cpus, spec.gpus, spec.nodes)
        with open(script, "w") as fh:
            fh.write(f"#!{sh}\n")
            fh.write(f"#flux: --nodes={spec.nodes}\n")
            fh.write(f"#flux: --nslots={alloc['num_slots']}\n")
            fh.write(f"#flux: --cores-per-slot={alloc['cores_per_slot']}\n")
            #    fh.write(f"#flux: --gpus-per-slot={alloc['gpus_per_slot']}\n")
            fh.write(f"#flux: --time-limit={duration}s\n")
            if spec.output:
                fh.write(f"#flux: --output={spec.output}\n")
            if spec.error:
                fh.write(f"#flux: --error={spec.output}\n")
            for arg in self.config["default_options"]:
                fh.write(f"#flux: {arg}\n")
            for arg in spec.submit_args:
                fh.write(f"#flux: {arg}\n")
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
        return FluxProcess(s.commands[0])

    def get_alloc_settings(
        self, cpus: int | None = None, gpus: int | None = None, nodes: int | None = None
    ) -> dict[str, Any]:
        alloc: dict[str, Any] = {}
        if nodes is not None:
            if cpus is None:
                cpus = nodes * self.backend.count_per_node("cpu")
            if gpus is None:
                gpus = nodes * self.backend.count_per_node("gpu", default=0)
        else:
            cpus = cpus or 1
            gpus = gpus or 0
            nodes = self.backend.nodes_required(max_cpus=cpus, max_gpus=gpus)

        alloc["num_nodes"] = nodes
        alloc["num_slots"] = nodes
        if nodes > 1:
            cpus = max(1, math.ceil(cpus / nodes))
            gpus = max(0, math.ceil(gpus / nodes))

        alloc["cores_per_slot"] = cpus
        alloc["gpus_per_slot"] = gpus
        return alloc

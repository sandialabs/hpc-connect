# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import os
import shutil

from .launch import LaunchAdapter
from .launch import LaunchSpec

logger = logging.getLogger("hpc_connect.mpi.launch")


class MPIExecAdapter(LaunchAdapter):
    name: str = "mpiexec"

    def join_specs(self, specs: list["LaunchSpec"]) -> list[str]:
        name = self.config.get("exec") or "mpiexec"
        exec = shutil.which(name)
        if exec is None:
            raise ValueError(f"{name}: executable not found on PATH")
        if len(specs) > 1:
            return self._join_mpmd(exec, specs)
        return self._join_spmd(exec, specs[0])

    def _join_spmd(self, exec: str, spec: LaunchSpec) -> list[str]:
        argv = [os.fsdecode(exec)]
        view = self.backend.resource_view(ranks=spec.processes)
        for opt in self.config["default_options"]:
            argv.append(self.expand_one(opt, **view))
        launch_opts, program_opts = spec.partition()
        for opt in launch_opts:
            argv.append(self.expand_one(opt, **view))
        for opt in self.config["pre_options"]:
            argv.append(self.expand_one(opt, **view))
        for opt in program_opts:
            argv.append(self.expand_one(opt, **view))
        return argv

    def _join_mpmd(self, exec: str, specs: list["LaunchSpec"]) -> list[str]:
        argv = [os.fsdecode(exec)]
        np = sum(spec.processes for spec in specs if spec.processes)
        argv.extend(self.config["mpmd"]["global_options"])
        argv.extend(self.config["default_options"])
        view = self.backend.resource_view(ranks=np)
        for opt in self.config["mpmd"]["global_options"]:
            argv.append(self.expand_one(opt, **view))
        for opt in self.config["default_options"]:
            argv.append(self.expand_one(opt, **view))

        for spec in specs:
            view = self.backend.resource_view(ranks=spec.processes or 1)
            for opt in self.config["mpmd"]["local_options"]:
                argv.append(self.expand_one(opt, **view))
            launch_opts, program_opts = spec.partition()
            for opt in launch_opts:
                argv.append(self.expand_one(opt, **view))
            for opt in self.config["pre_options"]:
                argv.append(self.expand_one(opt, **view))
            for opt in program_opts:
                argv.append(self.expand_one(opt, **view))
            argv.append(":")
        if argv[-1] == ":":
            argv.pop()
        return argv

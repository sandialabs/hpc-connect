import io
import os
import shutil
from typing import Any

from hpc_connect.launch import LaunchAdapter
from hpc_connect.launch import LaunchSpec


class SrunAdapter(LaunchAdapter):
    def join_specs(self, specs: list["LaunchSpec"]) -> list[str]:
        """Count the total number of processes and write a srun.conf file to
        split the jobs across ranks

        """
        assert os.path.basename(self.config.exec) == "srun"
        exec = shutil.which(self.config.exec)
        if exec is None:
            raise ValueError(f"{self.config.exec}: executable not found on PATH")
        if len(specs) > 1:
            return self._join_mpmd(exec, specs)
        return self._join_spmd(exec, specs[0])

    def _join_spmd(self, exec: str, spec: LaunchSpec) -> list[str]:
        argv = [os.fsdecode(exec)]
        view = self.backend.resource_view(ranks=spec.processes)
        for opt in self.config.default_options:
            argv.append(self.expand(opt, **view))
        launch_opts, program_opts = spec.partition()
        for opt in launch_opts:
            argv.append(self.expand(opt, **view))
        for opt in self.config.pre_options:
            argv.append(self.expand(opt, **view))
        for opt in program_opts:
            argv.append(self.expand(opt, **view))
        return argv

    def _join_mpmd(self, exec: str, specs: list["LaunchSpec"]) -> list[str]:
        np: int = 0
        fp = io.StringIO()
        for spec in specs:
            ranks: str
            p = spec.processes
            if p:
                ranks = f"{np}-{np + p - 1}"
                np += p
            else:
                ranks = str(np)
                np += 1
            launch_opts, program_opts = spec.partition()
            fp.write(ranks)
            view = self.backend.resource_view(ranks=p)
            for opt in self.config.mpmd.local_options:
                fp.write(f" {self.expand(opt, **view)}")
            for opt in launch_opts:
                fp.write(f" {self.expand(opt, **view)}")
            for opt in self.config.pre_options:
                fp.write(f" {self.expand(opt, **view)}")
            for opt in program_opts:
                fp.write(f" {self.expand(opt, **view)}")
            fp.write("\n")
        file = "launch-multi-prog.conf"
        with open(file, "w") as fh:
            fh.write(fp.getvalue())
        cmd = [os.fsdecode(exec)]
        view = self.backend.resource_view(ranks=np)
        for opt in self.config.mpmd.global_options:
            cmd.append(self.expand(opt, **view))
        for opt in self.config.default_options:
            cmd.append(self.expand(opt, **view))
        cmd.extend([f"-n{np}", "--multi-prog", file])
        return cmd

    @staticmethod
    def expand(arg: str, **kwargs: Any) -> str:
        return str(arg) % kwargs

import io
import os
import shutil

from ..config import Config
from ..hookspec import hookimpl
from ..submit import slurm
from .base import HPCLauncher
from .base import LaunchSpecs


class SrunLauncher(HPCLauncher):
    def __init__(self, config: Config | None = None) -> None:
        super().__init__(config=config)
        if not self.exec.endswith("srun"):
            raise ValueError("SrunLauncher: expected exec = srun")
        if self.config.get("machine:resources") is None:
            if sinfo := slurm.read_sinfo():
                self.config.set("machine:resources", [sinfo])

    @staticmethod
    def matches(arg: str) -> bool:
        return os.path.basename(arg) == "srun"

    def join_specs(
        self,
        launchspecs: "LaunchSpecs",
        local_options: list[str] | None = None,
        global_options: list[str] | None = None,
        pre_options: list[str] | None = None,
    ) -> list[str]:
        """Count the total number of processes and write a srun.conf file to
        split the jobs across ranks

        """
        if len(launchspecs) <= 1:
            return super().join_specs(
                launchspecs, local_options=local_options, global_options=global_options
            )

        local_options = list(local_options or [])
        local_options.extend(self.config.get("launch:local_options"))
        global_options = list(global_options or [])
        global_options.extend(self.config.get("launch:default_options"))
        pre_options = list(pre_options or [])
        pre_options.extend(self.config.get("launch:pre_options"))

        np: int = 0
        fp = io.StringIO()
        for p, spec in launchspecs:
            ranks: str
            if p is not None:
                ranks = f"{np}-{np + p - 1}"
                np += p
            else:
                ranks = str(np)
                np += 1
            i = self.argp(spec)
            fp.write(ranks)
            for opt in local_options:
                fp.write(f" {self.expand(opt, np=np)}")
            for opt in pre_options:
                fp.write(f" {self.expand(opt, np=np)}")
            for arg in spec[i:]:
                fp.write(f" {self.expand(arg, np=p)}")
            fp.write("\n")
        file = "launch-multi-prog.conf"
        with open(file, "w") as fh:
            fh.write(fp.getvalue())
        cmd = [os.fsdecode(self.exec)]
        required_resources = self.config.compute_required_resources(ranks=np)
        for opt in global_options:
            cmd.append(self.expand(opt, **required_resources))
        cmd.extend([f"-n{np}", "--multi-prog", file])
        return cmd

    @staticmethod
    def argp(args: list[str]) -> int:
        for i, arg in enumerate(args):
            if shutil.which(arg):
                return i
        return -1


@hookimpl
def hpc_connect_launcher(config: Config) -> HPCLauncher | None:
    if SrunLauncher.matches(config.get("launch:exec")):
        return SrunLauncher(config=config)
    return None

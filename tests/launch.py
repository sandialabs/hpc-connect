import os
import yaml
from contextlib import contextmanager
import hpc_connect
from hpc_connect.config import load as load_config


@contextmanager
def working_dir(dirname):
    save_cwd = os.getcwd()
    os.makedirs(dirname, exist_ok=True)
    try:
        os.chdir(dirname)
        yield
    finally:
        os.chdir(save_cwd)

@contextmanager
def envmods(**kwargs):
    try:
        save_env = os.environ.copy()
        for key in os.environ:
            if key.startswith("HPCC_"):
                os.environ.pop(key)
        os.environ.update(kwargs)
        yield
    finally:
        os.environ.clear()
        os.environ.update(save_env)

mock_bin = os.path.join(os.path.dirname(__file__), "mock")


class LaunchConfig:
    def __init__(self, **kwargs):
        self.data = load_config()
        for key, val in kwargs.items():
            self.data["launch"][key] = val
    @property
    def mappings(self):
        return self.data["launch"]["mappings"]
    @property
    def numproc_flag(self):
        return self.data["launch"]["numproc_flag"]
    @property
    def default_flags(self):
        return self.data["launch"]["default_flags"]
    @property
    def exec(self):
        return self.data["launch"]["exec"]



def test_envar_config(capfd):
    with envmods(HPCC_LAUNCH_EXEC="srun", HPCC_LAUNCH_NUMPROC_FLAG="-np"):
        hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        assert captured.out.strip() == f"{mock_bin}/srun -np 4 -flag file executable --option"

    with envmods(HPCC_LAUNCH_EXEC="mpiexec", HPCC_LAUNCH_NUMPROC_FLAG="-np", HPCC_LAUNCH_DEFAULT_FLAGS="--map-by ppr:%(np)d:cores"):
        hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        assert captured.out.strip() == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"


def test_file_config(tmpdir, capfd):

    with working_dir(str(tmpdir)):
        with open("hpc_connect.yaml", "w") as fh:
            yaml.dump({"hpc_connect": {"launch": {"exec": "srun", "numproc_flag": "-np"}}}, fh)
        with envmods(HPCC_CONFIG_FILE="hpc_connect.yaml"):
            hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            assert captured.out.strip() == f"{mock_bin}/srun -np 4 -flag file executable --option"

        with open("hpc_connect.yaml", "w") as fh:
            yaml.dump({"hpc_connect": {"launch": {"exec": "mpiexec", "numproc_flag": "-np", "default_flags": "--map-by ppr:%(np)d:cores"}}}, fh)
        with envmods(HPCC_CONFIG_FILE="hpc_connect.yaml"):
            hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            assert captured.out.strip() == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"

        with open("hpc_connect.yaml", "w") as fh:
            yaml.dump({"hpc_connect": {"launch": {"exec": "mpiexec", "numproc_flag": "-np", "mappings": {"-flag": "-xflag"}}}}, fh)
        with envmods(HPCC_CONFIG_FILE="hpc_connect.yaml"):
            hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            assert captured.out.strip() == f"{mock_bin}/mpiexec -np 4 -xflag file executable --option"


def test_default(capfd):
    hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    assert captured.out.strip() == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_mappings(capfd):
    hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    assert captured.out.strip() == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_mpmd():
    from hpc_connect import _launch
    config = LaunchConfig()
    parser = _launch.ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
    argv = ["-n", "4", "-flag", "file", "ls", ":", "-n", "5", "ls", "-la"]
    args = parser.parse_args(argv)
    cmd = _launch.join_args(args, config=config.data)
    assert " ".join(cmd) == "mpiexec -n 4 -flag file ls : -n 5 ls -la"


def test_srun_mpmd(tmpdir):
    from hpc_connect import _launch
    with working_dir(str(tmpdir)):
        config = LaunchConfig(exec="srun")
        parser = _launch.ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
        argv = ["-n", "4", "ls", ":", "-n", "5", "ls", "-la"]
        args = parser.parse_args(argv)
        cmd = _launch.join_args(args, config=config.data)
        assert " ".join(cmd) == "srun -n9 --multi-prog launch-multi-prog.conf"
        with open("launch-multi-prog.conf") as fh:
            assert fh.read().strip() == "0-3 ls\n4-8 ls -la"


def test_mapped(tmpdir):
    from hpc_connect import _launch
    with working_dir(str(tmpdir)):
        config = LaunchConfig(mappings={"--x": "--y"}, numproc_flag="-np")
        parser = _launch.ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        args = parser.parse_args(argv)
        cmd = _launch.join_args(args, config=config.data)
        assert " ".join(cmd) == "mpiexec --y 4 --y=5 -np=7 ls"


def test_mapped_suppressed(tmpdir):
    from hpc_connect import _launch
    with working_dir(str(tmpdir)):
        config = LaunchConfig(mappings={"--x": "SUPPRESS"}, numproc_flag="-np")
        parser = _launch.ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        args = parser.parse_args(argv)
        cmd = _launch.join_args(args, config=config.data)
        assert " ".join(cmd) == "mpiexec -np=7 ls"


def test_count_procs(tmpdir):
    from hpc_connect import _launch
    with working_dir(str(tmpdir)):
        config = LaunchConfig()
        parser = _launch.ArgumentParser(mappings=config.mappings, numproc_flag=config.numproc_flag)
        argv = ["-n", "4", "ls", ":", "-n=5", "ls"]
        args = parser.parse_args(argv)
        assert args.processes[0] == 4
        assert args.processes[1] == 5

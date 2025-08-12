import os
from contextlib import contextmanager

import yaml

import hpc_connect


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


def launch(args, **kwargs):
    launcher = hpc_connect.get_launcher()
    launcher(args, **kwargs)


def test_envar_config(capfd):
    env = {"HPCC_LAUNCH_EXEC": "srun", "HPCC_LAUNCH_NUMPROC_FLAG": "-np"}
    with envmods(**env):
        launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/srun -np 4 -flag file executable --option"
    env = {
        "HPCC_LAUNCH_EXEC": "mpiexec",
        "HPCC_LAUNCH_NUMPROC_FLAG": "-np",
        "HPCC_LAUNCH_LOCAL_OPTIONS": "--map-by ppr:%(np)d:cores",
    }
    with envmods(**env):
        launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"


def test_file_config(tmpdir, capfd):
    with envmods(HPC_CONNECT_GLOBAL_CONFIG="hpc_connect.yaml"):
        with working_dir(str(tmpdir)):
            with open("hpc_connect.yaml", "w") as fh:
                yaml.dump({"hpc_connect": {"launch": {"exec": "srun", "numproc_flag": "-np"}}}, fh)
            launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/srun -np 4 -flag file executable --option"

            with open("hpc_connect.yaml", "w") as fh:
                yaml.dump(
                    {
                        "hpc_connect": {
                            "launch": {
                                "exec": "mpiexec",
                                "numproc_flag": "-np",
                                "local_options": "--map-by ppr:%(np)d:cores",
                            }
                        }
                    },
                    fh,
                )
            launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"

            with open("hpc_connect.yaml", "w") as fh:
                yaml.dump(
                    {
                        "hpc_connect": {
                            "launch": {
                                "exec": "mpiexec",
                                "numproc_flag": "-np",
                                "mappings": {"-flag": "-xflag"},
                            }
                        }
                    },
                    fh,
                )

            launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/mpiexec -np 4 -xflag file executable --option"


def test_default(capfd):
    launch(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    out = captured.out.strip()
    assert out == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_envar_mappings(capfd):
    with envmods(HPCC_LAUNCH_MAPPINGS="-spam:-ham,-eggs:-bacon"):
        launch(
            ["-n", "4", "-spam", "ham", "-eggs", "bacon", "executable", "--option"]
        )
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec -n 4 -ham ham -bacon bacon executable --option"


def test_mpmd():
    launcher = hpc_connect.get_launcher()
    argv = ["-n", "4", "-flag", "file", "ls", ":", "-n", "5", "ls", "-la"]
    cmd = launcher.prepare_command_line(argv)
    assert os.path.basename(cmd[0]) == "mpiexec"
    assert " ".join(cmd[1:]) == "-n 4 -flag file ls : -n 5 ls -la"


def test_srun_mpmd(tmpdir):
    with working_dir(str(tmpdir)):
        config = hpc_connect.config.Config()
        config.set("launch:exec", "srun")
        launcher = hpc_connect.get_launcher(config)
        argv = ["-n", "4", "ls", ":", "-n", "5", "ls", "-la"]
        cmd = launcher.prepare_command_line(argv)
        assert " ".join(cmd) == "srun -n9 --multi-prog launch-multi-prog.conf"
        with open("launch-multi-prog.conf") as fh:
            assert fh.read().strip() == "0-3 ls\n4-8 ls -la"


def test_mapped(tmpdir):
    with working_dir(str(tmpdir)):
        launcher = hpc_connect.get_launcher()
        launcher.config.set("launch:mappings", {"--x": "--y"})
        launcher.config.set("launch:numproc_flag", "-np")
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        cmd = launcher.prepare_command_line(argv)
        assert os.path.basename(cmd[0]) == "mpiexec"
        assert " ".join(cmd[1:]) == "--y 4 --y=5 -np=7 ls"


def test_mapped_suppressed(tmpdir):
    with working_dir(str(tmpdir)):
        launcher = hpc_connect.get_launcher()
        launcher.config.set("launch:mappings", {"--x": "SUPPRESS"})
        launcher.config.set("launch:numproc_flag", "-np")
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        cmd = launcher.prepare_command_line(argv)
        assert os.path.basename(cmd[0]) == "mpiexec"
        assert " ".join(cmd[1:]) == "-np=7 ls"


def test_count_procs(tmpdir):
    with working_dir(str(tmpdir)):
        from hpc_connect.launch.base import ArgumentParser
        parser = ArgumentParser(mappings={}, numproc_flag="-n")
        argv = ["-n", "4", "ls", ":", "-n=5", "ls"]
        args = parser.parse_args(argv)
        assert args.processes[0] == 4
        assert args.processes[1] == 5

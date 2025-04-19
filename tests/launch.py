import os
from contextlib import contextmanager

import yaml

import hpc_connect
import hpc_connect._launch
import hpc_connect.config


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


def test_envar_config(capfd):
    env = {"HPCC_LAUNCH_EXEC": "srun", "HPCC_LAUNCH_NUMPROC_FLAG": "-np"}
    with envmods(**env):
        with hpc_connect.config.override():
            hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            assert captured.out.strip() == f"{mock_bin}/srun -np 4 -flag file executable --option"
    env = {
        "HPCC_LAUNCH_EXEC": "mpiexec",
        "HPCC_LAUNCH_NUMPROC_FLAG": "-np",
        "HPCC_LAUNCH_LOCAL_OPTIONS": "--map-by ppr:%(np)d:cores",
    }
    with envmods(**env):
        with hpc_connect.config.override():
            hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            assert (
                captured.out.strip()
                == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"
            )


def test_file_config(tmpdir, capfd):
    with envmods(HPCC_CONFIG="hpc_connect.yaml"):
        with working_dir(str(tmpdir)):
            with open("hpc_connect.yaml", "w") as fh:
                yaml.dump({"hpc_connect": {"launch": {"exec": "srun", "numproc_flag": "-np"}}}, fh)
            with hpc_connect.config.override():
                hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
                captured = capfd.readouterr()
                assert captured.out.strip() == f"{mock_bin}/srun -np 4 -flag file executable --option"

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
            with hpc_connect.config.override():
                hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
                captured = capfd.readouterr()
                assert (
                    captured.out.strip()
                    == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"
                )

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
            with hpc_connect.config.override():
                hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
                captured = capfd.readouterr()
                assert (
                    captured.out.strip()
                    == f"{mock_bin}/mpiexec -np 4 -xflag file executable --option"
                )


def test_default(capfd):
    with hpc_connect.config.override():
        hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        assert captured.out.strip() == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_envar_mappings(capfd):
    with envmods(HPCC_LAUNCH_MAPPINGS="-spam:-ham,-eggs:-bacon"):
        with hpc_connect.config.override():
            hpc_connect.launch(
                ["-n", "4", "-spam", "ham", "-eggs", "bacon", "executable", "--option"]
            )
            captured = capfd.readouterr()
            assert (
                captured.out.strip()
                == f"{mock_bin}/mpiexec -n 4 -ham ham -bacon bacon executable --option"
            )


def test_mpmd():
    with hpc_connect.config.override():
        parser = hpc_connect._launch.ArgumentParser(
            mappings=hpc_connect.config.get("launch:mappings"),
            numproc_flag=hpc_connect.config.get("launch:numproc_flag"),
        )
        argv = ["-n", "4", "-flag", "file", "ls", ":", "-n", "5", "ls", "-la"]
        args = parser.parse_args(argv)
        cmd = hpc_connect._launch.join_args(args)
        assert " ".join(cmd) == "mpiexec -n 4 -flag file ls : -n 5 ls -la"


def test_srun_mpmd(tmpdir):
    with hpc_connect.config.override():
        hpc_connect.config.set("launch:exec", "srun")
        with working_dir(str(tmpdir)):
            parser = hpc_connect._launch.ArgumentParser(
                mappings=hpc_connect.config.get("launch:mappings"),
                numproc_flag=hpc_connect.config.get("launch:numproc_flag"),
            )
            argv = ["-n", "4", "ls", ":", "-n", "5", "ls", "-la"]
            args = parser.parse_args(argv)
            cmd = hpc_connect._launch.join_args(args)
            assert " ".join(cmd) == "srun -n9 --multi-prog launch-multi-prog.conf"
            with open("launch-multi-prog.conf") as fh:
                assert fh.read().strip() == "0-3 ls\n4-8 ls -la"


def test_mapped(tmpdir):
    with hpc_connect.config.override():
        hpc_connect.config.set("launch:mappings", {"--x": "--y"})
        hpc_connect.config.set("launch:numproc_flag", "-np")
        with working_dir(str(tmpdir)):
            parser = hpc_connect._launch.ArgumentParser(
                mappings=hpc_connect.config.get("launch:mappings"),
                numproc_flag=hpc_connect.config.get("launch:numproc_flag"),
            )
            argv = ["--x", "4", "--x=5", "-n=7", "ls"]
            args = parser.parse_args(argv)
            cmd = hpc_connect._launch.join_args(args)
            assert " ".join(cmd) == "mpiexec --y 4 --y=5 -np=7 ls"


def test_mapped_suppressed(tmpdir):
    with hpc_connect.config.override():
        hpc_connect.config.set("launch:mappings", {"--x": "SUPPRESS"})
        hpc_connect.config.set("launch:numproc_flag", "-np")
        with working_dir(str(tmpdir)):
            parser = hpc_connect._launch.ArgumentParser(
                mappings=hpc_connect.config.get("launch:mappings"),
                numproc_flag=hpc_connect.config.get("launch:numproc_flag"),
            )
            argv = ["--x", "4", "--x=5", "-n=7", "ls"]
            args = parser.parse_args(argv)
            cmd = hpc_connect._launch.join_args(args)
            assert " ".join(cmd) == "mpiexec -np=7 ls"


def test_count_procs(tmpdir):
    with hpc_connect.config.override():
        with working_dir(str(tmpdir)):
            parser = hpc_connect._launch.ArgumentParser(
                mappings=hpc_connect.config.get("launch:mappings"),
                numproc_flag=hpc_connect.config.get("launch:numproc_flag"),
            )
            argv = ["-n", "4", "ls", ":", "-n=5", "ls"]
            args = parser.parse_args(argv)
            assert args.processes[0] == 4
            assert args.processes[1] == 5

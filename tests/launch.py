import os
import shlex
from contextlib import contextmanager
from pathlib import Path

import yaml

import hpc_connect


@contextmanager
def working_dir(dirname: Path):
    save_cwd = Path.cwd()
    dirname.mkdir(parents=True, exist_ok=True)
    try:
        os.chdir(dirname)
        yield
    finally:
        os.chdir(save_cwd)


@contextmanager
def envmods(**kwargs):
    save_env = os.environ.copy()
    try:
        for key in os.environ:
            if key.startswith("HPC_CONNECT_"):
                os.environ.pop(key)
        os.environ.update(kwargs)
        yield
    finally:
        os.environ.clear()
        os.environ.update(save_env)


mock_bin = os.path.join(os.path.dirname(__file__), "mock")


def launch(args, env=True, files=True, **kwargs):
    config = hpc_connect.Config.from_defaults(env=env, files=files)
    launcher = hpc_connect.get_launcher(config=config)
    launcher(args, **kwargs)


def test_envar_config(capfd):
    env = {
        "HPC_CONNECT_BACKEND": "slurm",
        "HPC_CONNECT_LAUNCH_EXEC": "srun",
        "HPC_CONNECT_LAUNCH_NUMPROC_FLAG": "-np",
    }
    with envmods(**env):
        launch(["-n", "4", "-flag", "file", "executable", "--option"], files=False)
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/srun -np 4 -flag file executable --option"
    env = {
        "HPC_CONNECT_BACKEND": "local",
        "HPC_CONNECT_LAUNCH_EXEC": "mpiexec",
        "HPC_CONNECT_LAUNCH_NUMPROC_FLAG": "-np",
        "HPC_CONNECT_LAUNCH_DEFAULT_OPTIONS": "--map-by ppr:%(np)d:cores",
    }
    with envmods(**env):
        launch(["-n", "4", "-flag", "file", "executable", "--option"], files=False)
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"


def test_file_config(tmpdir, capfd):
    with envmods(HPC_CONNECT_GLOBAL_CONFIG="hpc_connect.yaml"):
        workspace = Path(tmpdir.strpath)
        with working_dir(workspace):
            with open(workspace / "hpc_connect.yaml", "w") as fh:
                yaml.dump(
                    {
                        "hpc_connect": {
                            "backend": "slurm",
                            "launch": {"exec": "srun", "numproc_flag": "-np"},
                        }
                    },
                    fh,
                )
            launch(["-n", "4", "-flag", "file", "executable", "--option"], env=False)
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/srun -np 4 -flag file executable --option"

            with open(workspace / "hpc_connect.yaml", "w") as fh:
                yaml.dump(
                    {
                        "hpc_connect": {
                            "backend": "local",
                            "launch": {
                                "exec": "mpiexec",
                                "numproc_flag": "-np",
                                "default_options": "--map-by ppr:%(np)d:cores",
                            },
                        }
                    },
                    fh,
                )
            launch(["-n", "4", "-flag", "file", "executable", "--option"], env=False)
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert (
                out == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"
            )

            with open("hpc_connect.yaml", "w") as fh:
                yaml.dump(
                    {
                        "hpc_connect": {
                            "backend": "slurm",
                            "launch": {
                                "exec": "mpiexec",
                                "numproc_flag": "-np",
                                "mappings": {"-flag": "-xflag"},
                            },
                        }
                    },
                    fh,
                )

            launch(["-n", "4", "-flag", "file", "executable", "--option"], env=False)
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/mpiexec -np 4 -xflag file executable --option"


def test_default(capfd):
    launch(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    out = captured.out.strip()
    assert out == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_envar_mappings(capfd):
    with envmods(HPC_CONNECT_LAUNCH_MAPPINGS="-spam:-ham,-eggs:-bacon"):
        launch(["-n", "4", "-spam", "ham", "-eggs", "bacon", "executable", "--option"])
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec -n 4 -ham ham -bacon bacon executable --option"


def test_mpmd(capfd, tmpdir):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    with working_dir(workspace):
        with open("foo.sh", "a") as fh:
            fh.write("#/usr/bin/env sh\necho $@\n")
        os.chmod("foo.sh", 0o750)
        launch(["-n", "4", "-flag", "file", "./foo.sh", ":", "-n", "5", "./foo.sh", "-a"])
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec -n 4 -flag file ./foo.sh : -n 5 ./foo.sh -a"


def test_srun_mpmd(capfd, tmpdir):
    workspace = Path(tmpdir.strpath)
    with working_dir(workspace):
        config = hpc_connect.config.Config().from_defaults(
            overrides={"backend": "slurm", "launch": {"exec": "srun"}}
        )
        with open("foo.sh", "a") as fh:
            fh.write("#/usr/bin/env sh\necho $@\n")
        os.chmod("foo.sh", 0o750)
        launcher = hpc_connect.get_launcher(config=config)
        argv = ["-n", "4", "./foo.sh", ":", "-n", "5", "./foo.sh", "-a"]
        launcher(argv)
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/srun -n9 --multi-prog launch-multi-prog.conf"
        with open("launch-multi-prog.conf") as fh:
            text = fh.read().strip()
            print(text)
            assert text == "0-3 ./foo.sh\n4-8 ./foo.sh -a"


def test_mapped(capfd, tmpdir):
    with working_dir(Path(tmpdir.strpath)):
        config = hpc_connect.Config.from_defaults(
            overrides={"launch": {"numproc_flag": "-np", "mappings": {"--x": "--y"}}}
        )
        launcher = hpc_connect.get_launcher(config=config)
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        launcher(argv)
        captured = capfd.readouterr()
        out = captured.out.strip()
        cmd = shlex.split(out)
        assert os.path.basename(cmd[0]) == "mpiexec"
        assert " ".join(cmd[1:]) == "--y 4 --y=5 -np=7 ls"


def test_mapped_suppressed(capfd, tmpdir):
    with working_dir(Path(tmpdir.strpath)):
        config = hpc_connect.Config.from_defaults(
            overrides={"launch": {"numproc_flag": "-np", "mappings": {"--x": "SUPPRESS"}}}
        )
        launcher = hpc_connect.get_launcher(config=config)
        argv = ["--x", "4", "--x=5", "-n=7", "ls"]
        launcher(argv)
        captured = capfd.readouterr()
        out = captured.out.strip()
        cmd = shlex.split(out)
        assert os.path.basename(cmd[0]) == "mpiexec"
        assert " ".join(cmd[1:]) == "-np=7 ls"


def test_count_procs(tmpdir):
    with working_dir(Path(tmpdir.strpath)):
        from hpc_connect.launch import ArgumentParser

        parser = ArgumentParser(mappings={}, numproc_flag="-n")
        argv = ["-n", "4", "ls", ":", "-n=5", "ls"]
        args = parser.parse_args(argv)
        assert args[0].processes == 4
        assert args[1].processes == 5

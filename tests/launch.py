import os
from contextlib import contextmanager
from pathlib import Path

import yaml

import hpc_connect


import pytest

@pytest.fixture(scope="function", autouse=True)
def reset_config():
    hpc_connect.config.reset()


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


def file_config(backend: str) -> str:
    cfg = f"""\
hpc_connect:
  backend: {backend}
  backends:
  - name: my.slurm
    type: slurm
    launch:
      type: mpi
      numproc_flag: -np
      exec: mpiexec
  - name: my.local
    type: local
    launch:
      type: mpi
      numproc_flag: -np
      default_options: --map-by ppr:%(np)d:cores
"""
    return cfg


def test_file_config_1(tmpdir, capfd):
    workspace = Path(tmpdir.strpath)
    with envmods(HPC_CONNECT_GLOBAL_CONFIG=(workspace / "hpc_connect.yaml").as_posix()):
        with working_dir(workspace):
            with open(workspace / "hpc_connect.yaml", "w") as fh:
                fh.write(file_config("slurm"))
            backend = hpc_connect.get_backend()
            launcher = backend.launcher()
            launcher(["-n", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            print(captured.err)
            assert out == f"{mock_bin}/srun -n 4 -flag file executable --option"


def test_file_config_2(tmpdir, capfd):
    workspace = Path(tmpdir.strpath)
    with envmods(HPC_CONNECT_GLOBAL_CONFIG=(workspace / "hpc_connect.yaml").as_posix()):
        with working_dir(workspace):
            with open(workspace / "hpc_connect.yaml", "w") as fh:
                fh.write(file_config("my.local"))
            backend = hpc_connect.get_backend()
            launcher = backend.launcher()
            launcher(["-np", "4", "-flag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert (
                out == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"
            )

def test_file_config_3(tmpdir, capfd):
    workspace = Path(tmpdir.strpath)
    with envmods(HPC_CONNECT_GLOBAL_CONFIG=(workspace / "hpc_connect.yaml").as_posix()):
        with working_dir(workspace):
            with open(workspace / "hpc_connect.yaml", "w") as fh:
                fh.write(file_config("my.slurm"))
            backend = hpc_connect.get_backend()
            launcher = backend.launcher()
            launcher(["-np", "4", "-xflag", "file", "executable", "--option"])
            captured = capfd.readouterr()
            out = captured.out.strip()
            assert out == f"{mock_bin}/mpiexec -np 4 -xflag file executable --option"


def test_default(capfd):
    backend = hpc_connect.get_backend("local")
    launcher = backend.launcher()
    launcher(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    out = captured.out.strip()
    assert out == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"


def test_mpmd(capfd, tmpdir):
    workspace = Path(tmpdir.strpath)
    workspace.mkdir(parents=True, exist_ok=True)
    with working_dir(workspace):
        with open("foo.sh", "a") as fh:
            fh.write("#/usr/bin/env sh\necho $@\n")
        os.chmod("foo.sh", 0o750)
        backend = hpc_connect.get_backend("local")
        launcher = backend.launcher()
        launcher(["-n", "4", "-flag", "file", "./foo.sh", ":", "-n", "5", "./foo.sh", "-a"])
        captured = capfd.readouterr()
        out = captured.out.strip()
        assert out == f"{mock_bin}/mpiexec -n 4 -flag file ./foo.sh : -n 5 ./foo.sh -a"


def test_srun_mpmd(capfd, tmpdir):
    from hpcc_slurm.backend import SlurmBackend

    workspace = Path(tmpdir.strpath)
    with working_dir(workspace):
        backend = SlurmBackend()
        with open("foo.sh", "a") as fh:
            fh.write("#/usr/bin/env sh\necho $@\n")
        os.chmod("foo.sh", 0o750)
        launcher = backend.launcher()
        argv = ["-n", "4", "./foo.sh", ":", "-n", "5", "./foo.sh", "-a"]
        launcher(argv)
        captured = capfd.readouterr()
        print("HERE I AM", backend.default_config())
        print("HERE I AM", backend.config)
        print("HERE I AM", launcher.adapter.config)
        out = captured.out.strip()
        print(captured.err)
        assert out == f"{mock_bin}/srun -n9 --multi-prog launch-multi-prog.conf"
        with open("launch-multi-prog.conf") as fh:
            text = fh.read().strip()
            assert text == "0-3 ./foo.sh\n4-8 ./foo.sh -a"


def test_count_procs(tmpdir):
    with working_dir(Path(tmpdir.strpath)):
        from hpc_connect.launch import ArgumentParser

        parser = ArgumentParser(numproc_flag="-n")
        argv = ["-n", "4", "ls", ":", "-n=5", "ls"]
        args = parser.parse_args(argv)
        assert args[0].processes == 4
        assert args[1].processes == 5

import os
from contextlib import contextmanager
import hpc_connect


@contextmanager
def envmods(**kwargs):
    try:
        save_env = os.environ.copy()
        os.environ.update(kwargs)
        yield
    finally:
        os.environ.clear()
        os.environ.update(save_env)

mock_bin = os.path.join(os.path.dirname(__file__), "mock")


def test_envar_config(capfd):
    with envmods(HPCC_LAUNCH_EXEC="srun", HPCC_LAUNCH_NUMPROC_FLAG="-np"):
        hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        assert captured.out.strip() == f"{mock_bin}/srun -np 4 -flag file executable --option"

    with envmods(HPCC_LAUNCH_EXEC="mpiexec", HPCC_LAUNCH_NUMPROC_FLAG="-np", HPCC_LAUNCH_DEFAULT_FLAGS="--map-by ppr:%(np)d:cores"):
        hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
        captured = capfd.readouterr()
        assert captured.out.strip() == f"{mock_bin}/mpiexec --map-by ppr:4:cores -np 4 -flag file executable --option"


def test_default(capfd):
    hpc_connect.launch(["-n", "4", "-flag", "file", "executable", "--option"])
    captured = capfd.readouterr()
    assert captured.out.strip() == f"{mock_bin}/mpiexec -n 4 -flag file executable --option"

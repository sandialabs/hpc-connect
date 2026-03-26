import os

import hpc_connect.config


def test_config_launch_basic(tmpdir):
    cwd = os.getcwd()
    try:
        os.chdir(tmpdir.strpath)
        config = hpc_connect.config.Config()
        backend_cfg = {
            "name": "my-backend",
            "type": "local",
            "launch": {
                "type": "mpi",
                "default_options": ["-a", "-b"],
            }
        }
        config.set("backends", [backend_cfg])
        backend = config.backend("my-backend")
        assert backend is not None
        assert backend["launch"]["default_options"] == ["-a", "-b"]
    finally:
        os.chdir(cwd)

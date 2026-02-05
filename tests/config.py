import os

import hpc_connect.config


def test_config_launch_basic(tmpdir):
    cwd = os.getcwd()
    try:
        os.chdir(tmpdir.strpath)
        config = hpc_connect.config.Config.from_defaults(
            overrides={
                "launch": {
                    "exec": "my-mpiexec",
                    "default_options": ["-a", "-b"],
                    "mappings": {"-e": "-f"},
                    "mpiexec": {"default_options": ["-c", "-d"]},
                }
            }
        )
        assert config.launch.default_options == ["-a", "-b"]
        assert config.launch.mappings == {"-e": "-f"}
        assert config.launch.resolve("mpiexec").default_options == ["-c", "-d"]
        assert config.launch.resolve("mpiexec").mappings == {"-e": "-f"}
    finally:
        os.chdir(cwd)

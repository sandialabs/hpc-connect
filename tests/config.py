import os

import hpc_connect.config


def test_config_launch_basic(tmpdir):
    try:
        cwd = os.getcwd()
        os.chdir(tmpdir.strpath)

        config = hpc_connect.config.Config()
        config.set("launch:exec", "my-mpiexec")
        assert config.get("launch:exec") == "my-mpiexec"

        config.set("launch:local_flags", ["-a", "-b"])
        assert config.get("launch:local_flags") == ["-a", "-b"]
        config.add('launch:local_flags:["-c", "-d"]')
        assert config.get("launch:local_flags") == ["-a", "-b", "-c", "-d"]
        config.add('launch:local_flags:-e')
        assert config.get("launch:local_flags") == ["-a", "-b", "-c", "-d", "-e"]

        config.set("launch:mappings", {"-a": "-b", "-c": "-d"})
        assert config.get("launch:mappings") == {"-a": "-b", "-c": "-d"}
        config.add('launch:mappings:{"-e": "-f"}')
        assert config.get("launch:mappings") == {"-a": "-b", "-c": "-d", "-e": "-f"}
    finally:
        os.chdir(cwd)

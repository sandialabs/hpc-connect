import copy

import hpc_connect.config


def test_config_launch_basic():
    with hpc_connect.config.override():
        hpc_connect.config.set("launch:exec", "my-mpiexec")
        assert hpc_connect.config.get("launch:exec") == "my-mpiexec"

        hpc_connect.config.set("launch:default_local_options", ["-c", "-d"])
        assert hpc_connect.config.get("launch:default_local_options") == ["-c", "-d"]
        hpc_connect.config.update("launch:default_local_options", ["-a", "-b"])
        assert hpc_connect.config.get("launch:default_local_options") == ["-a", "-b", "-c", "-d"]

        hpc_connect.config.set("launch:mappings", {"-a": "-b", "-c": "-d"})
        assert hpc_connect.config.get("launch:mappings") == {"-a": "-b", "-c": "-d"}
        hpc_connect.config.update("launch:mappings", {"-e": "-f"})
        assert hpc_connect.config.get("launch:mappings") == {"-a": "-b", "-c": "-d", "-e": "-f"}


def test_config_reset():
    with hpc_connect.config.override():
        cfg = copy.deepcopy(hpc_connect.config._config)
        hpc_connect.config.set("launch:default_local_options", ["-a", "-b"])
        hpc_connect.config.update("launch:default_local_options", ["-a", "-b"])
        hpc_connect.config.restore_defaults()
        assert cfg == hpc_connect.config._config

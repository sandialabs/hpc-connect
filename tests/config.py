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
            "launch": {"type": "mpi", "default_options": ["-a", "-b"]},
        }
        config.set("backends", [backend_cfg])
        backend = config.backend("my-backend")
        assert backend is not None
        assert backend["launch"]["default_options"] == ["-a", "-b"]
    finally:
        os.chdir(cwd)


def test_overlay_from_mods_basic():
    overlay = hpc_connect.config.overlay_from_mods(["backends:[{'name':'x','type':'local'}]"])
    assert overlay == {"backends": "[{'name':'x','type':'local'}]"}


def test_apply_config_mods_merges_nested_values():
    data = {"debug": False, "backends": [{"name": "x", "type": "local"}]}
    result = hpc_connect.config.apply_config_mods(data, ["debug:true"])
    assert result["debug"] is True
    assert result["backends"] == [{"name": "x", "type": "local"}]

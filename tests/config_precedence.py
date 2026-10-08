# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
from pathlib import Path

import hpc_connect.config as config_mod


def test_config_scope_precedence_site_global_local(tmpdir):
    root = Path(tmpdir.strpath)
    site_file = root / "site.yaml"
    global_file = root / "global.yaml"
    local_file = root / "hpc_connect.yaml"

    site_file.write_text(
        """\
hpc_connect:
  debug: false
  backend: site.local
  backends:
    - name: site.local
      type: local
""",
        encoding="utf-8",
    )
    global_file.write_text(
        """\
hpc_connect:
  debug: true
  backend: global.local
  backends:
    - name: global.local
      type: local
""",
        encoding="utf-8",
    )
    local_file.write_text(
        """\
hpc_connect:
  backend: local.local
  backends:
    - name: local.local
      type: local
""",
        encoding="utf-8",
    )

    save_env = os.environ.copy()
    cwd = Path.cwd()
    try:
        os.chdir(root)
        os.environ["HPC_CONNECT_SITE_CONFIG"] = str(site_file)
        os.environ["HPC_CONNECT_GLOBAL_CONFIG"] = str(global_file)

        cfg = config_mod.Config()

        assert cfg["debug"] is True
        assert cfg["backend"] == "local.local"
        assert cfg.backend("site.local") is not None
        assert cfg.backend("global.local") is not None
        assert cfg.backend("local.local") is not None
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(save_env)


def test_hpc_connect_cfg64_overrides_file_scopes(tmpdir, clear_config_cache):
    root = Path(tmpdir.strpath)
    local_file = root / "hpc_connect.yaml"
    local_file.write_text(
        """\
hpc_connect:
  debug: false
  backend: file.local
  backends:
    - name: file.local
      type: local
""",
        encoding="utf-8",
    )

    save_env = os.environ.copy()
    cwd = Path.cwd()
    try:
        os.chdir(root)

        cfg = config_mod.Config()
        cfg.set("backend", "env.local")
        cfg.set("backends", [{"name": "env.local", "type": "local"}])
        snapshot = cfg.export()

        os.environ["HPC_CONNECT_CFG64"] = snapshot
        clear_config_cache()

        loaded = config_mod.Config()
        assert loaded["backend"] == "env.local"
        assert loaded.backend("env.local") is not None
        # set("backends", [...]) merges list entries rather than replacing the
        # existing file-provided list, so both backends remain present.
        assert loaded.backend("file.local") is not None
    finally:
        os.chdir(cwd)
        os.environ.clear()
        os.environ.update(save_env)

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from pathlib import Path

import hpc_connect.command


def _write_config(path: Path) -> None:
    path.write_text(
        """\
hpc_connect:
  backend: local
  backends:
    - type: local
      launch:
        default_options: []
""",
        encoding="utf-8",
    )


def test_make_parser_registers_config_and_launch_commands():
    parser = hpc_connect.command.make_parser()
    subparsers_actions = [
        action for action in parser._actions if action.__class__.__name__ == "_SubParsersAction"
    ]
    assert subparsers_actions
    choices = subparsers_actions[0].choices
    assert "config" in choices
    assert "info" in choices
    assert "launch" in choices


def test_command_main_info_returns_zero(capsys):
    rc = hpc_connect.command.main(["--info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Configuration" in out


def test_command_main_config_show_outputs_yaml(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    _write_config(cfg)
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["config", "show"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "hpc_connect" not in out
    assert "backends:" in out
    assert "backend: local" in out


def test_command_main_config_add_writes_local_scope(tmpdir, monkeypatch):
    root = Path(tmpdir.strpath)
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["config", "add", "--scope", "local", "backend:local"])

    assert rc == 0
    text = (root / "hpc_connect.yaml").read_text(encoding="utf-8")
    assert "hpc_connect:" in text
    assert "backend: local" in text


def test_command_main_launch_dryrun_uses_backend_owned_launch_path(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    _write_config(cfg)
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["launch", "--dryrun", "-n", "2", "executable"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out.endswith("mpiexec -n 2 executable")


def test_command_main_info_subcommand_shows_backend_node_groups(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    _write_config(cfg)
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: local" in out
    assert "Type: local" in out
    assert "Node groups: 1" in out
    assert "Group 1:" in out
    assert "cpu per node:" in out


def test_command_main_info_subcommand_works_without_backend_key_when_one_backend(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - type: local
      launch:
        default_options: []
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: local" in out
    assert "Type: local" in out


def test_command_main_info_subcommand_lists_all_backends_when_no_default(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - name: first.local
      type: local
      config:
        cpus_per_node: 2
      launch:
        default_options: []
    - name: second.local
      type: local
      config:
        cpus_per_node: 4
      launch:
        default_options: []
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: first.local" in out
    assert "Name: second.local" in out
    assert out.count("Group 1:") == 2


def test_command_main_info_subcommand_honors_top_level_backend_override(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - name: first.local
      type: local
      config:
        cpus_per_node: 2
      launch:
        default_options: []
    - name: second.local
      type: local
      config:
        cpus_per_node: 4
      launch:
        default_options: []
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["--backend", "second.local", "info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: second.local" in out
    assert "Name: first.local" not in out


def test_command_main_info_subcommand_honors_top_level_backend_type_override(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    cfg.write_text(
        """\
hpc_connect:
  backends:
    - name: my.local
      type: local
      launch:
        default_options: []
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["--backend", "local", "info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: local" in out or "Name: my.local" in out
    assert "Type: local" in out


def test_command_main_info_subcommand_falls_back_to_local_without_config(monkeypatch, capsys):
    monkeypatch.delenv("HPC_CONNECT_CFG64", raising=False)
    monkeypatch.delenv("HPC_CONNECT_GLOBAL_CONFIG", raising=False)
    monkeypatch.delenv("HPC_CONNECT_SITE_CONFIG", raising=False)

    rc = hpc_connect.command.main(["info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Name: local" in out
    assert "Type: local" in out

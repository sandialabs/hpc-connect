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
        type: mpi
        exec: mpiexec
        numproc_flag: -n
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


def test_command_main_launch_dryrun_uses_backend_owned_launch_path(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    _write_config(cfg)
    monkeypatch.chdir(root)

    rc = hpc_connect.command.main(["launch", "--dryrun", "-n", "2", "executable"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out.endswith("mpiexec -n 2 executable")

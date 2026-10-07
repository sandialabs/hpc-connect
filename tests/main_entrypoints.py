# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from pathlib import Path

import hpc_connect.__main__ as main_mod


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


def test_module_main_dispatches_to_command_info(capsys):
    rc = main_mod.main(["--info"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Overview" in out


def test_module_launch_prepends_launch_subcommand(tmpdir, monkeypatch, capsys):
    root = Path(tmpdir.strpath)
    cfg = root / "hpc_connect.yaml"
    _write_config(cfg)
    monkeypatch.chdir(root)

    rc = main_mod.launch(["--dryrun", "-n", "3", "executable"])
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out.endswith("mpiexec -n 3 executable")

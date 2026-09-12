# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import datetime
import importlib.util
import os

import pytest


def _load_pre_commit():
    """Load dev/pre_commit.py from the repo root regardless of install mode."""
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.normpath(os.path.join(here, ".."))
    dev_file = os.path.join(repo_root, "dev", "pre_commit.py")
    spec = importlib.util.spec_from_file_location("hpc_connect_dev.pre_commit", dev_file)
    assert spec is not None and spec.loader is not None, f"Cannot load {dev_file}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


pre_commit = _load_pre_commit()


def test_date_based_version_maps_year_minus_2000():
    assert pre_commit.date_based_version(datetime.date(2026, 9, 11)) == "26.9.11"
    assert pre_commit.date_based_version(datetime.date(2026, 10, 23)) == "26.10.23"
    assert pre_commit.date_based_version(datetime.date(2000, 1, 1)) == "0.1.1"


def _write_pyproject(tmp_path, version):
    p = tmp_path / "pyproject.toml"
    p.write_text(
        "[build-system]\n"
        'requires = ["setuptools>=64"]\n\n'
        "[project]\n"
        'name = "hpc-connect"\n'
        f'version = "{version}"\n'
    )
    return p


def test_read_pyproject_version(tmp_path):
    p = _write_pyproject(tmp_path, "26.9.2")
    assert pre_commit.read_pyproject_version(str(p)) == "26.9.2"


def test_write_pyproject_version_only_touches_version(tmp_path):
    p = _write_pyproject(tmp_path, "26.9.2")
    pre_commit.write_pyproject_version(str(p), "26.9.11")
    text = p.read_text()
    assert 'version = "26.9.11"' in text
    assert 'name = "hpc-connect"' in text  # other fields untouched
    assert pre_commit.read_pyproject_version(str(p)) == "26.9.11"


def test_write_pyproject_version_missing_raises(tmp_path):
    p = tmp_path / "pyproject.toml"
    p.write_text('[project]\nname = "x"\n')
    with pytest.raises(ValueError):
        pre_commit.write_pyproject_version(str(p), "26.9.11")


def test_run_check_passes_when_current(tmp_path, monkeypatch):
    _write_pyproject(tmp_path, "26.9.11")
    monkeypatch.chdir(tmp_path)
    rc = pre_commit.run(check=True, run_tests=False, date="2026-09-11")
    assert rc == 0


def test_run_check_fails_when_stale(tmp_path, monkeypatch):
    _write_pyproject(tmp_path, "26.9.2")
    monkeypatch.chdir(tmp_path)
    rc = pre_commit.run(check=True, run_tests=False, date="2026-09-11")
    assert rc == 1


def test_run_stamps_version(tmp_path, monkeypatch):
    p = _write_pyproject(tmp_path, "26.9.2")
    monkeypatch.chdir(tmp_path)
    rc = pre_commit.run(check=False, run_tests=False, date="2026-09-11")
    assert rc == 0
    assert pre_commit.read_pyproject_version(str(p)) == "26.9.11"

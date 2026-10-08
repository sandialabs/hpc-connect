# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os

import hpc_connect.version as version_mod


def test_version_from_pyproject_returns_zero_when_repo_missing(monkeypatch):
    monkeypatch.setattr(version_mod, "_find_repo_root", lambda start_dir: None)
    assert version_mod._version_from_pyproject() == "0.0.0"


def test_find_repo_root_walks_up(tmp_path):
    root = tmp_path / "repo"
    nested = root / "a" / "b" / "c"
    nested.mkdir(parents=True)
    (root / ".git").mkdir()

    result = version_mod._find_repo_root(str(nested))
    assert result == os.path.abspath(str(root))


def test_get_version_returns_base_when_metadata_already_has_local(monkeypatch):
    monkeypatch.setattr(version_mod, "_base_version", lambda: "26.10.8+local")
    monkeypatch.setattr(version_mod, "_find_repo_root", lambda start_dir: "/repo")
    assert version_mod.get_version() == "26.10.8+local"


def test_get_version_returns_base_when_git_label_fails(monkeypatch):
    monkeypatch.setattr(version_mod, "_base_version", lambda: "26.10.8")
    monkeypatch.setattr(version_mod, "_find_repo_root", lambda start_dir: "/repo")

    def boom():
        raise version_mod.CannotDetermineVersionFromGitError("boom")

    monkeypatch.setattr(version_mod, "git_local_label", boom)
    assert version_mod.get_version() == "26.10.8"


def test_git_is_dirty_false_when_diff_quiet_succeeds(monkeypatch):
    class Result:
        returncode = 0

    monkeypatch.setattr(version_mod.subprocess, "run", lambda *a, **k: Result())
    assert version_mod._git_is_dirty("/repo") is False


def test_git_is_dirty_true_when_diff_quiet_fails(monkeypatch):
    class Result:
        returncode = 1

    monkeypatch.setattr(version_mod.subprocess, "run", lambda *a, **k: Result())
    assert version_mod._git_is_dirty("/repo") is True

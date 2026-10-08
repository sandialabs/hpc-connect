# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import argparse
import importlib.util
from types import ModuleType

import hpc_connect.command as command_mod


def test_load_dev_command_no_git_dir(monkeypatch):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    monkeypatch.setattr(command_mod.ir, "files", lambda name: "/tmp/fake/site-packages/hpc_connect")
    monkeypatch.setattr(command_mod.os.path, "isdir", lambda path: False)

    command_mod._load_dev_command(subparsers)


def test_load_dev_command_missing_file(monkeypatch):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    monkeypatch.setattr(command_mod.ir, "files", lambda name: "/tmp/fake/site-packages/hpc_connect")
    monkeypatch.setattr(command_mod.os.path, "isdir", lambda path: True)
    monkeypatch.setattr(command_mod.os.path, "isfile", lambda path: False)

    command_mod._load_dev_command(subparsers)


def test_load_dev_command_registers_module(monkeypatch):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    class FakeLoader:
        def exec_module(self, mod):
            mod.description = "dev command"

            def setup_parser(p):
                p.add_argument("--flag", action="store_true")

            mod.setup_parser = setup_parser

    class FakeSpec:
        loader = FakeLoader()

    monkeypatch.setattr(command_mod.ir, "files", lambda name: "/tmp/fake/site-packages/hpc_connect")
    monkeypatch.setattr(command_mod.os.path, "isdir", lambda path: True)
    monkeypatch.setattr(command_mod.os.path, "isfile", lambda path: True)
    monkeypatch.setattr(importlib.util, "spec_from_file_location", lambda name, path: FakeSpec())
    monkeypatch.setattr(importlib.util, "module_from_spec", lambda spec: ModuleType("fake_dev"))

    command_mod._load_dev_command(subparsers)

    assert "fake_dev" in command_mod._commands

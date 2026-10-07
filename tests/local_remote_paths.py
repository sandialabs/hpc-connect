# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import os
from contextlib import contextmanager
from pathlib import Path

from hpc_connect.local import streamify as local_streamify
from hpcc_remote.process import streamify as remote_streamify


def test_local_streamify_accepts_basename_only(tmpdir):
    cwd = Path(tmpdir.strpath) / "cwd"
    cwd.mkdir(parents=True, exist_ok=True)
    with working_dir(cwd):
        fh = local_streamify("stdout.txt")
        assert fh is not None
        try:
            fh.write("hello\n")
        finally:
            fh.close()
        assert Path("stdout.txt").read_text(encoding="utf-8") == "hello\n"


def test_remote_streamify_accepts_basename_only(tmpdir):
    cwd = Path(tmpdir.strpath) / "cwd"
    cwd.mkdir(parents=True, exist_ok=True)
    with working_dir(cwd):
        fh = remote_streamify("stderr.txt")
        assert fh is not None
        try:
            fh.write("hello\n")
        finally:
            fh.close()
        assert Path("stderr.txt").read_text(encoding="utf-8") == "hello\n"


@contextmanager
def working_dir(path: Path):
    old = Path.cwd()
    try:
        os.chdir(path)
        yield path
    finally:
        os.chdir(old)

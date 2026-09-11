# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""``hpcc pre-commit``: run the test suite and stamp the release version.

This is the deliberate, run-it-yourself replacement for a git pre-commit hook
(hooks are too easy to forget to install).  It:

1. runs the full test suite; if anything fails it aborts without touching
   anything, and
2. rewrites the static ``version`` in ``pyproject.toml`` to the current
   date-based version ``YY.M.D`` (year minus 2000).

The date-based version is what gets published to PyPI and baked into the
package metadata, so keeping ``pyproject.toml`` current on every commit means a
build from a ``.git``-stripped tree (e.g. Spack) still reports the right
version.

Run it before committing::

    hpcc pre-commit

Or check without modifying anything (useful in CI)::

    hpcc pre-commit --check
"""

from __future__ import annotations

import argparse
import datetime
import os
import re
import subprocess
import sys

description = "Run tests and stamp pyproject.toml with the date-based version"
command_name = "pre-commit"

_VERSION_RE = re.compile(r'^(?P<prefix>version\s*=\s*")(?P<ver>[^"]*)(?P<suffix>")\s*$', re.MULTILINE)


def setup_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not modify pyproject.toml; exit nonzero if the version is not current.",
    )
    parser.add_argument(
        "--no-tests",
        action="store_true",
        help="Skip running the test suite (only stamp/check the version).",
    )
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Override the date used to compute the version (default: today).",
    )


def execute(config, args: argparse.Namespace) -> None:  # noqa: ANN001 - config unused
    rc = run(check=args.check, run_tests=not args.no_tests, date=args.date)
    if rc != 0:
        raise SystemExit(rc)


def date_based_version(date: datetime.date | None = None) -> str:
    """Return the ``YY.M.D`` version string for ``date`` (default: today)."""
    d = date or datetime.date.today()
    return f"{d.year - 2000}.{d.month}.{d.day}"


def find_repo_root(start: str | None = None) -> str:
    d = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.exists(os.path.join(d, "pyproject.toml")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            raise FileNotFoundError(
                "could not locate pyproject.toml above %r" % (start or os.getcwd())
            )
        d = parent


def read_pyproject_version(pyproject: str) -> str:
    with open(pyproject, encoding="utf-8") as fh:
        text = fh.read()
    m = _VERSION_RE.search(text)
    if not m:
        raise ValueError(f'no static `version = "..."` found in {pyproject}')
    return m.group("ver")


def write_pyproject_version(pyproject: str, new_version: str) -> None:
    with open(pyproject, encoding="utf-8") as fh:
        text = fh.read()
    if not _VERSION_RE.search(text):
        raise ValueError(f'no static `version = "..."` found in {pyproject}')
    new_text = _VERSION_RE.sub(rf"\g<prefix>{new_version}\g<suffix>", text, count=1)
    with open(pyproject, "w", encoding="utf-8") as fh:
        fh.write(new_text)


def _run_tests(repo_root: str) -> int:
    print("Running test suite (pytest)...", flush=True)
    proc = subprocess.run([sys.executable, "-m", "pytest"], cwd=repo_root)
    return proc.returncode


def run(*, check: bool = False, run_tests: bool = True, date: str | None = None) -> int:
    """Execute the pre-commit workflow.  Returns a process exit code."""
    repo_root = find_repo_root()
    pyproject = os.path.join(repo_root, "pyproject.toml")

    if date is not None:
        d = datetime.date.fromisoformat(date)
    else:
        d = datetime.date.today()
    expected = date_based_version(d)

    current = read_pyproject_version(pyproject)

    if check:
        if current == expected:
            print(f"pyproject.toml version is current: {current}")
            return 0
        print(
            f"pyproject.toml version is out of date: found {current!r}, "
            f"expected {expected!r}.\nRun `hpcc pre-commit` to update it.",
            file=sys.stderr,
        )
        return 1

    if run_tests:
        rc = _run_tests(repo_root)
        if rc != 0:
            print(
                "Tests failed; not updating the version. Fix the failures and re-run.",
                file=sys.stderr,
            )
            return rc

    if current == expected:
        print(f"Version already current ({current}); nothing to do.")
        return 0

    write_pyproject_version(pyproject, expected)
    print(f"Updated pyproject.toml version: {current} -> {expected}")
    print("Stage the change: git add pyproject.toml")
    return 0

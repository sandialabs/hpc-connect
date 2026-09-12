# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

"""``hpcc pre-commit``: run the full quality-gate and stamp the release version.

This is the deliberate, run-it-yourself replacement for a git pre-commit hook
(hooks are too easy to forget to install).  It runs, in order:

1. ``ruff format`` — auto-format source and tests.
2. ``ruff check --fix`` — lint and auto-fix imports / style.
3. ``bandit`` — security scan.
4. ``mypy`` — static type checking.
5. ``pytest`` — full test suite.
6. Version stamp — rewrites ``pyproject.toml`` to the current ``YY.M.D``
   date-based version.

Any step that fails aborts the remaining steps without modifying
``pyproject.toml``.

Run it before committing::

    hpcc pre-commit

Check-only mode (no writes, non-zero exit if version is stale)::

    hpcc pre-commit --check

Skip individual steps::

    hpcc pre-commit --no-format --no-lint --no-security --no-typecheck --no-tests
"""

from __future__ import annotations

import argparse
import datetime
import os
import re
import shutil
import subprocess
import sys

description = "Lint, type-check, test, and stamp pyproject.toml"
command_name = "pre-commit"

_VERSION_RE = re.compile(r'^(?P<prefix>version\s*=\s*")(?P<ver>[^"]*)(?P<suffix>")\s*$', re.MULTILINE)

_LICENSE_HEADER = (
    "# Copyright NTESS. See COPYRIGHT file for details.\n#\n# SPDX-License-Identifier: MIT\n\n"
)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def setup_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not modify anything; exit nonzero if the version is not current.",
    )
    parser.add_argument("--no-format", action="store_true", help="Skip ruff format.")
    parser.add_argument("--no-lint", action="store_true", help="Skip ruff check.")
    parser.add_argument("--no-security", action="store_true", help="Skip bandit security scan.")
    parser.add_argument("--no-typecheck", action="store_true", help="Skip mypy type checking.")
    parser.add_argument("--no-tests", action="store_true", help="Skip pytest.")
    parser.add_argument(
        "--date",
        default=None,
        metavar="YYYY-MM-DD",
        help="Override the date used to compute the version (default: today).",
    )


def execute(config, args: argparse.Namespace) -> None:  # noqa: ANN001 - config unused
    rc = run(
        check=args.check,
        do_format=not args.no_format,
        do_lint=not args.no_lint,
        do_security=not args.no_security,
        do_typecheck=not args.no_typecheck,
        run_tests=not args.no_tests,
        date=args.date,
    )
    if rc != 0:
        raise SystemExit(rc)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run(
    *,
    check: bool = False,
    do_format: bool = True,
    do_lint: bool = True,
    do_security: bool = True,
    do_typecheck: bool = True,
    run_tests: bool = True,
    date: str | None = None,
) -> int:
    """Execute the full pre-commit workflow.  Returns a process exit code."""
    repo_root = find_repo_root()
    pyproject = os.path.join(repo_root, "pyproject.toml")

    d = datetime.date.fromisoformat(date) if date is not None else datetime.date.today()
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

    src_paths = _source_paths(repo_root)

    if do_format:
        rc = _run_format(repo_root, src_paths)
        if rc != 0:
            return rc

    if do_lint:
        rc = _run_lint(repo_root, src_paths)
        if rc != 0:
            return rc

    if do_security:
        rc = _run_security(repo_root)
        if rc != 0:
            return rc

    if do_typecheck:
        rc = _run_typecheck(repo_root)
        if rc != 0:
            return rc

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
    else:
        write_pyproject_version(pyproject, expected)
        print(f"Updated pyproject.toml version: {current} -> {expected}")

    return 0


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------


def _source_paths(repo_root: str) -> list[str]:
    """Return the paths to pass to ruff (src/, tests/, dev/, bin/)."""
    paths = []
    for name in ("src", "tests", "dev", "bin"):
        p = os.path.join(repo_root, name)
        if os.path.exists(p):
            paths.append(p)
    return paths


def _run_format(repo_root: str, paths: list[str]) -> int:
    """Run ``ruff format`` over all source trees."""
    if not shutil.which("ruff"):
        print("WARNING: ruff not found on PATH; skipping format step.", file=sys.stderr)
        return 0
    print("ruff: formatting source...", flush=True)
    cp = subprocess.run(["ruff", "format", *paths], cwd=repo_root)
    return cp.returncode


def _run_lint(repo_root: str, paths: list[str]) -> int:
    """Run ``ruff check --fix`` over all source trees."""
    if not shutil.which("ruff"):
        print("WARNING: ruff not found on PATH; skipping lint step.", file=sys.stderr)
        return 0
    print("ruff: checking source...", flush=True)
    cp = subprocess.run(["ruff", "check", "--fix", *paths], cwd=repo_root)
    return cp.returncode


def _run_security(repo_root: str) -> int:
    """Run bandit security scan over ``src/``."""
    if not shutil.which("bandit"):
        print("WARNING: bandit not found on PATH; skipping security step.", file=sys.stderr)
        return 0
    print("bandit: security scan...", flush=True)
    cp = subprocess.run(["bandit", "-c", "pyproject.toml", "-r", "src/"], cwd=repo_root)
    return cp.returncode


def _run_typecheck(repo_root: str) -> int:
    """Run mypy type checking over ``src/``."""
    checker = shutil.which("mypy")
    if not checker:
        print("WARNING: mypy not found on PATH; skipping type-check step.", file=sys.stderr)
        return 0
    print("mypy: type checking source...", flush=True)
    cp = subprocess.run([checker, "src/"], cwd=repo_root)
    return cp.returncode


def _run_tests(repo_root: str) -> int:
    """Run the full pytest suite."""
    print("pytest: running tests...", flush=True)
    cp = subprocess.run([sys.executable, "-m", "pytest"], cwd=repo_root)
    return cp.returncode


# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------


def date_based_version(date: datetime.date | None = None) -> str:
    """Return the ``YY.M.D`` version string for ``date`` (default: today)."""
    d = date or datetime.date.today()
    return f"{d.year - 2000}.{d.month}.{d.day}"


def find_repo_root(start: str | None = None) -> str:
    """Walk up from *start* (default: cwd) until ``pyproject.toml`` is found."""
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
    """Return the version string from ``pyproject.toml``."""
    with open(pyproject, encoding="utf-8") as fh:
        text = fh.read()
    m = _VERSION_RE.search(text)
    if not m:
        raise ValueError(f'no static `version = "..."` found in {pyproject}')
    return m.group("ver")


def write_pyproject_version(pyproject: str, new_version: str) -> None:
    """Rewrite the version line in ``pyproject.toml`` in-place."""
    with open(pyproject, encoding="utf-8") as fh:
        text = fh.read()
    if not _VERSION_RE.search(text):
        raise ValueError(f'no static `version = "..."` found in {pyproject}')
    new_text = _VERSION_RE.sub(rf"\g<prefix>{new_version}\g<suffix>", text, count=1)
    with open(pyproject, "w", encoding="utf-8") as fh:
        fh.write(new_text)

#!/usr/bin/env python
import argparse
import glob
import logging
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from typing import Generator

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    p = subparsers.add_parser("build", help="Build wheels")
    p.add_argument("-l", action="store_true", default=False, help="Keep local")
    p = subparsers.add_parser("deploy", help="Upload to pypi index")
    p.add_argument("-u", help="twine username")
    p.add_argument("-p", help="twine password")
    args = parser.parse_args()
    if args.command == "build":
        return build_and_test(local=args.l)
    elif args.command == "deploy":
        return deploy(twine_username=args.u, twine_password=args.p)
    raise ValueError(f"Unknown command {args.command}")


def build_and_test(local: bool = False) -> None:
    build(local=local)
    test()


def build(local: bool = False) -> None:
    logger.info("Building wheel")
    with branch("production"):
        if os.path.exists("dist"):
            shutil.rmtree("dist")
        merge("main", local=local)
        if not local:
            subprocess.run(["git", "push", "origin", "production"], check=True)
        with virtual_env() as env:
            subprocess.run([env.bin.python3, "-m", "pip", "install", "build"], check=True)
            subprocess.run([env.bin.python3, "-m", "build", "--wheel"], check=True)
    logger.info("Done building wheels")
    return


def test() -> None:
    logger.info("Testing")
    assert os.path.exists("./dist"), "build wheels first"
    with virtual_env() as env:
        dist_dir = os.path.abspath("./dist")
        url = f"file://{dist_dir}"
        subprocess.run(
            [env.bin.python3, "-m", "pip", "install", "--find-links", url, "hpc-connect"], check=True
        )
        subprocess.run([env.bin.python3, "-m", "hpc_connect", "-h"], check=True)
    logger.info("Done testing")


def deploy(*, twine_username: str | None = None, twine_password: str | None = None) -> None:
    assert os.path.exists("./dist"), "build wheels first"
    logger.info("Deploying")
    with virtual_env() as env:
        if twine_username is not None:
            os.environ["TWINE_USERNAME"] = twine_username
        if "TWINE_USERNAME" not in os.environ:
            raise MissingEnvironmentVariableError("TWINE_USERNAME")
        if twine_password is not None:
            os.environ["TWINE_PASSWORD"] = twine_password
        if "TWINE_PASSWORD" not in os.environ:
            raise MissingEnvironmentVariableError("TWINE_PASSWORD")
        subprocess.run([env.bin.python3, "-m", "pip", "install", "twine"], check=True)
        wheels = glob.glob("./dist/*.whl")
        cmd = [
            env.bin.python3,
            "-m",
            "twine",
            "upload",
            "--skip-existing",
        ]
        cmd.extend(wheels)
        subprocess.run(cmd, check=True)
    logger.info("Done deploying")


class Prefix(str):
    def __getattr__(self, name: str) -> "Prefix":
        return Prefix(os.path.join(self, name))


@contextmanager
def virtual_env(preserve: bool = False) -> Generator[Prefix, None, None]:
    save_env = os.environ.copy()
    try:
        if os.path.exists("venv"):
            shutil.rmtree("venv")
        subprocess.run([sys.executable, "-m", "venv", "venv", "--system-site-packages"], check=True)
        save_env.pop("VIRTUAL_ENV", None)
        save_env.pop("VIRTUAL_ENV_PROMPT", None)
        save_env.pop("PYTHONHOME", None)
        venv = os.path.abspath("./venv")
        os.environ["VIRTUAL_ENV"] = venv
        os.environ["PATH"] = f"{venv}/bin:{os.environ['PATH']}"
        yield Prefix(venv)
    finally:
        os.environ.clear()
        os.environ.update(save_env)
        if not preserve:
            shutil.rmtree("venv")


@contextmanager
def branch(name: str) -> None:
    cwb = subprocess.check_output(["git", "branch", "--show-current"], encoding="utf-8").strip()
    try:
        subprocess.run(["git", "checkout", name], check=True)
        yield
    finally:
        subprocess.run(["git", "checkout", cwb], check=True)


def merge(name, local: bool = False):
    cwb = subprocess.check_output(["git", "branch", "--show-current"], encoding="utf-8").strip()
    git = lambda *args: subprocess.run(["git", *args], check=True)
    if not local:
        git("reset", "--hard", "HEAD")
        git("checkout", name)
        git("pull", "origin", "main")
        git("checkout", cwb)
    git("merge", "--no-ff", name, "-m", f"Merge remote-traching branch {name!r} into {cwb}")


class MissingEnvironmentVariableError(Exception):
    pass


if __name__ == "__main__":
    sys.exit(main())

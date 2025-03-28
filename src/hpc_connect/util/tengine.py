# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import importlib.resources

import jinja2

from .time import hhmmss


def make_template_env(*dirs: str) -> jinja2.Environment:
    """Returns a configured environment for template rendering."""
    template_dirs: set[str] = {str(importlib.resources.files("hpc_connect").joinpath("templates"))}
    template_dirs.update(dirs)
    loader = jinja2.FileSystemLoader(tuple(template_dirs))
    env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
    env.globals["hhmmss"] = hhmmss
    return env

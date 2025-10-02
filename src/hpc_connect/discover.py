# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import fnmatch
import json
import os
from typing import Any

import psutil

from .hookspec import hookimpl


@hookimpl(trylast=True, specname="hpc_connect_discover_resources")
def default_resource_set() -> list[dict[str, Any]]:
    local_resource = {"type": "cpu", "count": psutil.cpu_count()}
    socket_resource = {"type": "socket", "count": 1, "resources": [local_resource]}
    return [{"type": "node", "count": 1, "resources": [socket_resource]}]


@hookimpl(specname="hpc_connect_discover_resources")
def read_resources_from_hostfile() -> dict[str, list] | None:
    if file := os.getenv("HPC_CONNECT_HOSTFILE"):
        with open(file) as fh:
            data = json.load(fh)
        nodename = os.uname().nodename
        for pattern, rspec in data.items():
            if fnmatch.fnmatch(nodename, pattern):
                return rspec
    return None

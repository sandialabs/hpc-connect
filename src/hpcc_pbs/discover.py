# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import json
import logging
import os
import shutil
import subprocess
from typing import Any

logger = logging.getLogger(__name__)


def read_pbsnodes() -> list[dict[str, Any]] | None:
    if pbsnodes := shutil.which("pbsnodes"):
        args = [pbsnodes, "-a", "-F", "json"]
        allocated_nodes: list[str] | None = None
        if var := os.getenv("PBS_NODEFILE"):
            with open(var) as fh:
                allocated_nodes = [line.strip() for line in fh if line.split()]
        try:
            proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        except subprocess.CalledProcessError:
            return None
        else:
            resources: list[dict[str, Any]] = []
            data = json.loads(proc.stdout)
            config: dict[int, list[str]] = {}
            for nodename, nodeinfo in data["nodes"].items():
                if allocated_nodes is not None and nodename not in allocated_nodes:
                    continue
                cpus_on_node = nodeinfo["pcpus"]
                config.setdefault(cpus_on_node, []).append(nodename)
            for cpus_on_node, nodenames in config.items():
                resource: dict[str, Any] = {
                    "type": "node",
                    "count": len(nodenames),
                    "additional_properties": {"nodes": nodenames},
                    "resources": [
                        {
                            "type": "socket",
                            "count": 1,
                            "resources": [
                                {
                                    "type": "cpu",
                                    "count": cpus_on_node,
                                },
                            ],
                        },
                    ],
                }
                resources.append(resource)
            return resources
    return None

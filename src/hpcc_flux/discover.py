# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shutil
import subprocess
from typing import Any

logger = logging.getLogger("hpc_connect")


def parse_resource_info(output: str) -> dict[str, int] | None:
    """Parses the output from `flux resource info` and returns a dictionary of resource values.

    The expected output format is "1 Nodes, 32 Cores, 1 GPUs".

    Returns:
        dict: A dictionary containing the resource values with the following keys:
            - nodes (int): The number of nodes.
            - cpu (int): The number of CPU cores.
            - gpu (int): The number of GPU devices.
    """
    parts = output.split(", ")
    vals = [int(p.split()[0]) for p in parts]
    if len(vals) != 3:
        return None
    return {"nodes": vals[0], "cpu": vals[1], "gpu": vals[2]}


def read_resource_info() -> dict[str, Any] | None:
    if flux := shutil.which("flux"):
        try:
            output = subprocess.check_output([flux, "resource", "info"], encoding="utf-8")
        except subprocess.CalledProcessError:
            return None
        if totals := parse_resource_info(output):
            # assume homogenous resources
            nodes = totals["nodes"]
            info: dict = {
                "type": "node",
                "count": nodes,
                "resources": [
                    {
                        "type": "socket",
                        "count": 1,
                        "resources": [
                            {
                                "type": "cpu",
                                "count": int(totals["cpu"] / nodes),
                            },
                            {
                                "type": "gpu",
                                "count": int(totals["gpu"] / nodes),
                            },
                        ],
                    }
                ],
            }
            return info
    return None

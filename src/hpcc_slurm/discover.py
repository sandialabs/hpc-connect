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


def read_sinfo() -> dict[str, Any] | None:
    if sinfo := shutil.which("sinfo"):
        opts = [
            "%X",  # Number of sockets per node
            "%Y",  # Number of cores per socket
            "%Z",  # Number of threads per core
            "%c",  # Number of CPUs per node
            "%D",  # Number of nodes
            "%G",  # General resources
        ]
        format = " ".join(opts)
        args = [sinfo, "-o", format]
        try:
            proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        except subprocess.CalledProcessError:
            return None
        else:
            sockets_per_node: int
            cores_per_socket: int
            threads_per_core: int
            cpus_per_node: int
            node_count: int
            for line in proc.stdout.split("\n"):
                parts = line.split()
                if not parts:
                    continue
                elif parts and parts[0].startswith("SOCKETS"):
                    continue
                data = [safe_loads(part) for part in parts]
                sockets_per_node = data[0]
                cores_per_socket = data[1]
                threads_per_core = data[2]
                cpus_per_node = data[3]
                node_count = data[4]
                gres = data[5:]
                break
            else:
                raise ValueError(f"Unable to read sinfo output:\n{proc.stdout}")
            if var := os.getenv("SLURM_NNODES"):
                node_count = int(var)
            info: dict[str, Any] = {
                "type": "node",
                "count": node_count,
                "resources": [
                    {
                        "type": "socket",
                        "count": sockets_per_node,
                        "resources": [
                            {
                                "type": "cpu",
                                "count": int(cpus_per_node / sockets_per_node),
                            },
                        ],
                    }
                ],
                "additional_properties": {
                    "sockets_per_node": sockets_per_node,
                    "cores_per_socket": cores_per_socket,
                    "threads_per_core": threads_per_core,
                    "cpus_per_node": cpus_per_node,
                    "gres": " ".join(str(_) for _ in gres),
                },
            }
            for res in gres:
                if not res:
                    continue
                parts = res.split(":")
                resource: dict[str, Any] = {
                    "type": parts[0],
                    "count": safe_loads(parts[-1]),
                }
                if len(parts) > 2:
                    resource["gres"] = ":".join(parts[1:-1])
                info["resources"].append(resource)
            return info
    return None


def safe_loads(arg: str) -> Any:
    if arg == "(null)":
        return None
    if arg.endswith("+"):
        return safe_loads(arg[:-1])
    try:
        return json.loads(arg)
    except json.JSONDecodeError:
        return arg

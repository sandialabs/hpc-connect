# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
from typing import Any

logger = logging.getLogger("hpc_connect.slurm.discover")


def _parse_sinfo_line(line: str, cmd_line: str, allow_hyperthreading: bool = False) -> dict[str, Any]:
    parts = line.split()
    if len(parts) < 5:
        raise ValueError(f"Unable to parse sinfo output line: {line!r}")

    data = [safe_loads(part) for part in parts]
    sockets_per_node = data[0]
    cores_per_socket = data[1]
    threads_per_core = data[2]
    cpus_per_node = data[3]
    node_count = data[4]
    gres = data[5:]

    # By default we fill the schedulable CPU count from ``%c`` (the CPUs-per-node
    # Slurm reports, which on most sites equals physical cores).  When
    # hyperthreading is explicitly allowed, expose every hardware thread as a
    # schedulable CPU: sockets * cores_per_socket * threads_per_core.  Fall back
    # to ``%c`` if any factor is missing (e.g. ``(null)`` from sinfo).
    cpu_count = cpus_per_node
    if allow_hyperthreading:
        factors = (sockets_per_node, cores_per_socket, threads_per_core)
        if all(isinstance(f, int) for f in factors):
            cpu_count = sockets_per_node * cores_per_socket * threads_per_core
        else:
            logger.warning(
                "allow_hyperthreading requested but sinfo did not report integer "
                "sockets/cores/threads (%r); falling back to cpus_per_node=%r",
                factors,
                cpus_per_node,
            )

    info: dict[str, Any] = {
        "type": "node",
        "count": node_count,
        "resources": [{"type": "cpu", "count": cpu_count}],
        "additional_properties": {
            cmd_line: line,
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
        resource: dict[str, Any] = {"type": parts[0], "count": safe_loads(parts[-1])}
        if len(parts) > 2:
            resource["gres"] = ":".join(parts[1:-1])
        info["resources"].append(resource)
    return info


def read_sinfo(allow_hyperthreading: bool = False) -> list[dict[str, Any]] | None:
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
        args = [sinfo, "-e", "-o", format]
        try:
            proc = subprocess.run(args, check=True, encoding="utf-8", capture_output=True)
        except subprocess.CalledProcessError:
            return None
        else:
            resources: list[dict[str, Any]] = []
            for line in proc.stdout.split("\n"):
                parts = line.split()
                if not parts:
                    continue
                elif parts and parts[0].startswith("SOCKETS"):
                    continue
                cmd_line = shlex.join(args)
                resources.append(
                    _parse_sinfo_line(line, cmd_line, allow_hyperthreading=allow_hyperthreading)
                )

            if not resources:
                raise ValueError(f"Unable to read sinfo output:\n{proc.stdout}")

            if var := os.getenv("SLURM_NNODES"):
                if len(resources) == 1:
                    resources[0]["count"] = int(var)
                else:
                    logger.debug(
                        "Ignoring SLURM_NNODES=%s for heterogeneous sinfo output with %d node groups",
                        var,
                        len(resources),
                    )
            return resources
    return None


def safe_loads(arg: str) -> Any:
    if arg == "(null)":
        return None
    if ":" in arg:
        arg = strip_gres_suffixes(arg)
    if arg.endswith("+"):
        return safe_loads(arg[:-1])
    try:
        return json.loads(arg)
    except json.JSONDecodeError:
        return arg


def strip_gres_suffix(gres: str) -> str:
    """Remove trailing socket affinity patterns like (S:0-1) from GRES strings.

    Examples:
        "gpu:a40:1(S:0-1)" -> "gpu:a40:1"
        "nic:mlx5:1(S:0)" -> "nic:mlx5:1"
        "mem:128G(S:0-3)" -> "mem:128G"
        "gpu:h100:4" -> "gpu:h100:4" (no change)
    """
    return re.sub(r"\s*\([^)]*\)\s*$", "", gres)


def strip_gres_suffixes(gres_field: str) -> str:
    """Handle comma-separated GRES fields, stripping suffixes from each.

    Examples:
        "gpu:a40:1(S:0-1),nic:mlx5:1(S:0)" -> "gpu:a40:1,nic:mlx5:1"
    """
    return ",".join(strip_gres_suffix(x.strip()) for x in gres_field.split(","))

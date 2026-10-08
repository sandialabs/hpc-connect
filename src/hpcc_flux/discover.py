# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import logging
import shutil
import subprocess
from typing import Any

logger = logging.getLogger("hpc_connect.flux.discover")

RESOURCE_LIST_FORMAT = "{nnodes}|{ncores}|{ngpus}|{state}|{nodelist}"


def _resource_spec(nodes: int, cpus: int, gpus: int, **props: Any) -> dict[str, Any] | None:
    if nodes <= 0:
        return None
    if cpus < 0 or gpus < 0:
        return None
    if cpus % nodes != 0 or gpus % nodes != 0:
        return None

    resources: list[dict[str, int | str]] = [{"type": "cpu", "count": cpus // nodes}]
    if gpus > 0:
        resources.append({"type": "gpu", "count": gpus // nodes})

    return {
        "type": "node",
        "count": nodes,
        "resources": resources,
        "additional_properties": props or None,
    }


def parse_resource_list(output: str) -> list[dict[str, Any]] | None:
    resources: list[dict[str, Any]] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 4)
        if len(parts) != 5:
            return None
        try:
            nnodes = int(parts[0])
            ncores = int(parts[1])
            ngpus = int(parts[2])
        except ValueError:
            return None

        resource = _resource_spec(
            nnodes, ncores, ngpus, state=parts[3], nodelist=parts[4], flux_resource_list_line=line
        )
        if resource is None:
            return None
        resources.append(resource)
    return resources or None


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


def read_resource_info() -> list[dict[str, Any]] | None:
    if flux := shutil.which("flux"):
        try:
            output = subprocess.check_output(
                [flux, "resource", "list", "-s", "all", "-n", "-o", RESOURCE_LIST_FORMAT],
                encoding="utf-8",
            )
        except subprocess.CalledProcessError:
            output = ""
        if resources := parse_resource_list(output):
            return resources

        try:
            output = subprocess.check_output([flux, "resource", "info"], encoding="utf-8")
        except subprocess.CalledProcessError:
            return None
        if totals := parse_resource_info(output):
            # `flux resource info` is totals-only.  Avoid inventing a uniform multi-node
            # topology from aggregate counts; it is only safe when the instance contains a
            # single node.
            if resource := _resource_spec(
                totals["nodes"], totals["cpu"], totals["gpu"], flux_resource_info=output.strip()
            ):
                if totals["nodes"] == 1:
                    return [resource]
                logger.debug(
                    "Ignoring totals-only flux resource info for multi-node topology: %s",
                    output.strip(),
                )
    return None

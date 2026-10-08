import argparse

from .. import get_backend

description = "Show backend and node-group resource information"


def setup_parser(parser: argparse.ArgumentParser) -> None:
    return None


def execute(config: argparse.Namespace, args: argparse.Namespace) -> None:
    if config.get("backend"):
        print(format_backend_info(get_backend(config.get("backend"))))
        return

    entries = config.get("backends") or []
    if not entries:
        print(format_backend_info(get_backend("local")))
        return

    if len(entries) == 1:
        selector = entries[0].get("name") or entries[0]["type"]
        print(format_backend_info(get_backend(selector)))
        return

    blocks: list[str] = []
    for entry in entries:
        selector = entry.get("name") or entry["type"]
        blocks.append(format_backend_info(get_backend(selector)))
    print("\n\n".join(blocks))


def format_backend_info(backend) -> str:
    lines: list[str] = []
    lines.append(f"Name: {backend.name}")
    lines.append(f"Type: {backend.type}")
    lines.append(f"Homogeneous: {'yes' if backend.is_homogeneous() else 'no'}")
    lines.append(f"Node groups: {len(backend.topology.node_groups())}")

    for i, group in enumerate(backend.topology.node_groups(), start=1):
        lines.append(f"Group {i}:")
        lines.append(f"  Nodes: {int(group.count)}")
        child_resources = group.data.get("resources", []) or []
        if child_resources:
            for child in child_resources:
                lines.append(f"  {child['type']} per node: {child['count']}")
        else:
            lines.append("  Resources: none")

    return "\n".join(lines)

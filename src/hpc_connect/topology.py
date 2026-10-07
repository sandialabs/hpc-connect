# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from dataclasses import dataclass
from typing import Any
from typing import Iterable


class HeterogeneousTopologyError(ValueError):
    pass


@dataclass(frozen=True)
class ResourceEntry:
    type: str
    count: Any
    parent_type: str | None
    parent_counts: tuple[int, ...]
    children: tuple[str, ...]
    data: dict[str, Any]


@dataclass(frozen=True)
class Topology:
    entries: tuple[ResourceEntry, ...]

    @classmethod
    def from_resource_specs(cls, resource_specs: Iterable[dict[str, Any]]) -> "Topology":
        entries: list[ResourceEntry] = []
        for rspec in resource_specs:
            entries.extend(_walk_resources(rspec))
        return cls(entries=tuple(entries))

    def by_type(self, rtype: str) -> list[ResourceEntry]:
        return [entry for entry in self.entries if entry.type == rtype]

    def node_groups(self) -> list[ResourceEntry]:
        return self.by_type("node")

    def total(self, rtype: str) -> int:
        total = 0
        for entry in self.by_type(rtype):
            if entry.type == "node":
                total += int(entry.count)
                continue
            if not entry.parent_counts:
                continue
            multiplier = int(entry.count)
            for count in reversed(entry.parent_counts[:-1]):
                multiplier *= int(count)
            total += multiplier
        return total

    def max_per_node(self, rtype: str) -> int:
        node_groups = self.node_groups()
        if not node_groups:
            raise ValueError("topology has no node groups")
        return max(_per_node_count(group.data, rtype) for group in node_groups)

    def min_per_node(self, rtype: str) -> int:
        node_groups = self.node_groups()
        if not node_groups:
            raise ValueError("topology has no node groups")
        return min(_per_node_count(group.data, rtype) for group in node_groups)

    def is_homogeneous(self) -> bool:
        node_groups = self.node_groups()
        if len(node_groups) <= 1:
            return True
        first = _signature(node_groups[0].data)
        return all(_signature(group.data) == first for group in node_groups[1:])

    def uniform_per_node(self, rtype: str) -> int:
        if not self.is_homogeneous():
            raise HeterogeneousTopologyError(f"resource {rtype!r} is not uniform across node groups")
        node_groups = self.node_groups()
        if not node_groups:
            raise ValueError("topology has no node groups")
        return _per_node_count(node_groups[0].data, rtype)


def _signature(rspec: dict[str, Any]) -> tuple[Any, ...]:
    children = tuple(_signature(child) for child in rspec.get("resources", []) or [])
    return (rspec.get("type"), rspec.get("count"), children)


def _per_node_count(rspec: dict[str, Any], rtype: str) -> int:
    total = 0

    def walk(spec: dict[str, Any], multiplier: int = 1) -> None:
        nonlocal total
        count = spec.get("count")
        if not isinstance(count, int):
            return
        next_multiplier = multiplier * count
        if spec.get("type") == rtype:
            total += next_multiplier
        for child in spec.get("resources", []) or []:
            walk(child, next_multiplier)

    for child in rspec.get("resources", []) or []:
        walk(child, 1)
    return total


def _walk_resources(
    rspec: dict[str, Any], *, parent_type: str | None = None, parent_counts: tuple[int, ...] = ()
) -> list[ResourceEntry]:
    count = rspec["count"]
    counts = (*parent_counts, count) if isinstance(count, int) else parent_counts
    children = tuple(str(child["type"]) for child in rspec.get("resources", []) or [])
    entries = [
        ResourceEntry(
            type=str(rspec["type"]),
            count=count,
            parent_type=parent_type,
            parent_counts=counts,
            children=children,
            data=rspec,
        )
    ]
    for child in rspec.get("resources", []) or []:
        entries.extend(_walk_resources(child, parent_type=str(rspec["type"]), parent_counts=counts))
    return entries

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

from dataclasses import dataclass
from typing import Any
from typing import Iterable


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

# Copyright NTESS. See COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

import base64
import json
import zlib
from typing import Any


def serialize(obj: Any) -> str:
    string = str(json.dumps(obj, separators=(",", ":"), sort_keys=True))
    return compress64(string)


def deserialize(raw: str) -> Any:
    string = expand64(raw)
    return json.loads(string)


def compress64(string: str) -> str:
    compressed = zlib.compress(string.encode("utf-8"), level=zlib.Z_BEST_COMPRESSION)
    return base64.urlsafe_b64encode(compressed).decode("utf-8")


def expand64(raw: str) -> str:
    bytes_str = base64.urlsafe_b64decode(raw.encode("utf-8"))
    return zlib.decompress(bytes_str).decode("utf-8")

"""Canonical schema hashing for the Feed wire protocol.

Clients and servers produce byte-identical canonical JSON so schema hashes and
filter rules agree. The algorithm:

1. Build a JSON object: ``"$schema_name"`` plus one entry per field, value = type
   descriptor.
2. Serialize with object keys sorted **case-insensitively** at every nesting
   level, with no whitespace.
3. SHA-256 the UTF-8 bytes; hex-encode.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json_string(value: Any) -> str:
    """Canonical JSON string with object keys sorted case-insensitively.

    This is protocol behavior; change it only as part of a compatible protocol
    update.
    """
    if isinstance(value, dict):
        keys = sorted(value.keys(), key=lambda k: k.lower())
        parts = []
        for key in keys:
            parts.append(json.dumps(key, ensure_ascii=False))
            parts.append(":")
            parts.append(canonical_json_string(value[key]))
            parts.append(",")
        if parts:
            parts.pop()  # drop trailing comma
        return "{" + "".join(parts) + "}"
    if isinstance(value, list):
        return "[" + ",".join(canonical_json_string(v) for v in value) + "]"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    # int / float
    return json.dumps(value)


def compute_schema_hash(schema: Any) -> str:
    """Compute the schema hash: canonicalize, SHA-256, hex-encode.

    Returns a 64-character lowercase hex string.
    """
    serialized = canonical_json_string(schema)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

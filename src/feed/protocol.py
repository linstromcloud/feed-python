"""Wire-format construction: turning merged fields into the schema definition and
``data`` payload, and assembling the telemetry batch JSON.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from .fields import Field
from .schema import compute_schema_hash


class ProcessedEvent:
    """An event after merge + hashing, ready to be placed in a batch."""

    __slots__ = (
        "seq",
        "delivery_ticket",
        "channel",
        "schema_hash",
        "schema_def",
        "data",
    )

    def __init__(
        self,
        seq: int,
        delivery_ticket: int,
        channel: str,
        schema_hash: str,
        schema_def: Dict[str, Any],
        data: Dict[str, Any],
    ) -> None:
        self.seq = seq
        self.delivery_ticket = delivery_ticket
        self.channel = channel
        self.schema_hash = schema_hash
        self.schema_def = schema_def
        self.data = data


def build_event(
    schema_name: str, fields: List[Field]
) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
    """Build the schema definition object and ``data`` object from a schema name
    and merged fields, then compute the hash. Returns
    ``(schema_hash, schema_def, data)``.

    Schema and field names are lowercased so hashes and downstream tables are
    identical across clients, and server-authored rule keys compare against
    lowercased event data. Duplicate per-record fields are rejected before this
    point; when state and record fields share a name, the record field wins.
    """
    schema_def: Dict[str, Any] = {"$schema_name": schema_name.lower()}
    data: Dict[str, Any] = {}
    for f in fields:
        name = f.name  # already lowercased in Field.__init__
        schema_def[name] = f.type_descriptor()
        data[name] = f.data_value()
    schema_hash = compute_schema_hash(schema_def)
    return schema_hash, schema_def, data


def build_batch_json(session_id: str, events: List[ProcessedEvent]) -> bytes:
    """Assemble the telemetry batch JSON for a set of processed events.

    The ``schemas`` map contains exactly the distinct schemas referenced by the
    events. Returns the serialized JSON bytes (uncompressed).
    """
    schemas: Dict[str, Any] = {}
    event_arr: List[Dict[str, Any]] = []
    for ev in events:
        if ev.schema_hash not in schemas:
            schemas[ev.schema_hash] = ev.schema_def
        event_arr.append(
            {
                "schema_hash": ev.schema_hash,
                "session_sequence_num": ev.seq,
                "channel": ev.channel,
                "data": ev.data,
            }
        )
    batch = {"session_id": session_id, "schemas": schemas, "events": event_arr}
    return json.dumps(batch, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

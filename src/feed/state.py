"""Copy-on-write session state.

State fields persist across emits and are attached to every event. Mutation is
copy-on-write: each setter publishes a new immutable snapshot (a fresh tuple)
under a lock, while ``snapshot`` reads the current reference without locking
(reference reads/writes are atomic under CPython's GIL). So ``emit`` only ever
captures a cheap reference, never the field data.

Names are lowercased on set so state keys compare case-insensitively throughout
the protocol.
"""

from __future__ import annotations

import threading
from typing import Dict, List, Tuple

from .fields import Field, FieldType


class StateStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._current: Tuple[Field, ...] = ()

    def snapshot(self) -> Tuple[Field, ...]:
        """Cheap reference to the current snapshot, captured at emit time."""
        return self._current

    def set(self, name: str, ftype: FieldType, value) -> None:
        key = name.lower()
        with self._lock:
            fields = list(self._current)
            for i, f in enumerate(fields):
                if f.name == key:
                    fields[i] = Field(key, ftype, value)
                    break
            else:
                fields.append(Field(key, ftype, value))
            self._current = tuple(fields)

    def remove(self, name: str) -> None:
        key = name.lower()
        with self._lock:
            if not any(f.name == key for f in self._current):
                return
            self._current = tuple(f for f in self._current if f.name != key)

    def has(self, name: str) -> bool:
        key = name.lower()
        return any(f.name == key for f in self._current)


def merge(state: Tuple[Field, ...], event: List[Field]) -> List[Field]:
    """Merge a state snapshot with an event's per-emit fields.

    Event fields shadow state fields with the same (already-lowercased) name. The
    result preserves state order first, then event-only fields, each name once.
    Ordering is irrelevant to the hash (which sorts keys) but stable for
    debugging.
    """
    event_by_name: Dict[str, Field] = {f.name: f for f in event}
    out: List[Field] = []
    seen = set()
    for sf in state:
        ef = event_by_name.get(sf.name)
        out.append(ef if ef is not None else sf)
        seen.add(sf.name)
    for ef in event:
        if ef.name not in seen:
            out.append(event_by_name[ef.name])  # last value wins on duplicates
            seen.add(ef.name)
    return out

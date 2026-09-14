"""Bounded JSON sizing and in-memory event admission."""

import json
import threading
from collections.abc import Mapping


class EventTooLarge(ValueError):
    pass


def check_size(value, limit):
    """Measure JSON without first copying arrays or escaping a giant string."""
    remaining = limit
    active = set()

    def consume_bytes(size):
        nonlocal remaining
        remaining -= size
        if remaining < 0:
            raise EventTooLarge(f"event exceeds max_event_bytes={limit}")

    def visit(item):
        if isinstance(item, str):
            consume_bytes(2)
            if len(item) > remaining:
                raise EventTooLarge(f"event exceeds max_event_bytes={limit}")
            for offset in range(0, len(item), 4096):
                consume_bytes(
                    len(
                        json.dumps(
                            item[offset : offset + 4096], ensure_ascii=False
                        ).encode()
                    )
                    - 2
                )
        elif isinstance(item, (Mapping, list, tuple)):
            identity = id(item)
            if identity in active:
                raise ValueError("circular event data")
            consume_bytes(2 + max(0, len(item) - 1))
            if len(item) > remaining:
                raise EventTooLarge(f"event exceeds max_event_bytes={limit}")
            active.add(identity)
            try:
                if isinstance(item, Mapping):
                    for key, value in item.items():
                        visit(key)
                        consume_bytes(1)
                        visit(value)
                else:
                    for value in item:
                        visit(value)
            finally:
                active.remove(identity)
        elif item is None or item is True:
            consume_bytes(4)
        elif item is False:
            consume_bytes(5)
        elif isinstance(item, (int, float)):
            consume_bytes(len(str(item)))

    visit(value)
    return limit - remaining


def encode_record(value, limit):
    check_size(value, limit)
    data = json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    if len(data) > limit:
        raise EventTooLarge(f"event exceeds max_event_bytes={limit}")
    return data


class Admission:
    def __init__(self, memory_limit, wake):
        self.memory_limit, self.wake = memory_limit, wake
        self.condition = threading.Condition()
        self.used = 0
        self.accepting = True
        self.error = ""

    def reserve(self, size):
        with self.condition:
            if not self.accepting or self.error or self.used + size > self.memory_limit:
                self.wake.set()
                return False
            self.used += size
            return True

    def release(self, size):
        with self.condition:
            self.used -= size
            self.condition.notify_all()

    def stop(self):
        with self.condition:
            self.accepting = False
            self.condition.notify_all()

    def storage_error(self, reason):
        with self.condition:
            self.error = reason
            self.condition.notify_all()

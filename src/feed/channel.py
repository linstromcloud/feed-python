"""Per-channel emitter-side state: the bounded queue to the worker, the
sequence-number counter, and an optional token-bucket rate limiter.
"""

from __future__ import annotations

import queue
import threading
import time


class QueuedEvent:
    def __init__(self, ticket, payload, channel):
        self.delivery_ticket = ticket
        self.payload = payload
        self.channel = channel
        self.reserved = False


class _TokenBucket:
    """Simple token bucket guarding the emit rate."""

    __slots__ = ("_lock", "_tokens", "_max", "_refill_per_sec", "_last")

    def __init__(self, max_events_per_second: int) -> None:
        self._lock = threading.Lock()
        self._max = float(max_events_per_second)
        self._tokens = float(max_events_per_second)
        self._refill_per_sec = float(max_events_per_second)
        self._last = time.monotonic()

    def try_take(self) -> bool:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._tokens + elapsed * self._refill_per_sec, self._max)
            self._last = now
            if self._tokens < 1.0:
                return False
            self._tokens -= 1.0
            return True


class ChannelHandle:
    """Channel admission retains count limits, sequencing and rate limiting."""

    def __init__(self, settings, delivery, admission=None, max_event_bytes=4 * 1024**2):
        self.settings, self._delivery = settings, delivery
        self._admission = admission
        self._max_event_bytes = max_event_bytes
        self.queue = queue.Queue(maxsize=max(1, settings.queue_capacity))
        self._seq = 0
        self._last_warning = 0
        self._lock = threading.Lock()
        self._rate = (
            _TokenBucket(settings.max_events_per_second)
            if settings.max_events_per_second > 0
            else None
        )

    def try_emit(self, schema_name, event_fields, state):
        return self._emit(schema_name, event_fields, state, None)

    def emit_wait(self, schema_name, event_fields, state, timeout):
        return self._emit(schema_name, event_fields, state, max(0, timeout))

    def _warn(self, message):
        import logging

        now = time.monotonic()
        if now - self._last_warning >= 1:
            logging.getLogger("feed").warning("[feed] %s; record rejected", message)
            self._last_warning = now

    def _emit(self, schema_name, event_fields, state, timeout):
        from .limits import EventTooLarge, encode_record
        from .protocol import build_event
        from .state import merge

        deadline = time.monotonic() + (timeout or 0)
        admission = self._admission
        if admission is None or not admission.accepting:
            return False
        schema_hash, schema_def, data = build_event(
            schema_name, merge(state, event_fields)
        )
        try:
            body = encode_record(
                {
                    "channel": self.settings.name.lower(),
                    "schema_hash": schema_hash,
                    "schema_def": schema_def,
                    "data": data,
                },
                self._max_event_bytes,
            )
        except EventTooLarge as exc:
            self._warn(str(exc))
            return False
        rate_taken = self._rate is None
        while True:
            # Capacity waits happen outside this lock: a waiting large record
            # cannot prevent a smaller record from using available capacity.
            with self._lock:
                if not admission.accepting or admission.error:
                    return False
                if not rate_taken:
                    rate_taken = self._rate.try_take()
                if rate_taken and not self.queue.full():
                    ticket = self._delivery.begin()
                    payload = (b'{"ticket":%d,"seq":%d,' % (ticket, self._seq)) + body[
                        1:
                    ]
                    if len(payload) > self._max_event_bytes:
                        self._delivery.reject(ticket)
                        self._warn(
                            f"event exceeds max_event_bytes={self._max_event_bytes}"
                        )
                        return False
                    if admission.reserve(len(payload)):
                        self._seq += 1
                        self.queue.put_nowait(
                            QueuedEvent(ticket, payload, self.settings.name)
                        )
                        admission.wake.set()
                        return True
                    self._delivery.reject(ticket)
                remaining = deadline - time.monotonic()
                if timeout is None or remaining <= 0:
                    if rate_taken:
                        self._seq += 1
                    self._warn("channel or byte capacity unavailable")
                    return False
            with admission.condition:
                admission.condition.wait(min(remaining, 0.05))

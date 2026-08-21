"""Per-channel emitter-side state: the bounded queue to the worker, the
sequence-number counter, and an optional token-bucket rate limiter.
"""

from __future__ import annotations

import itertools
import queue
import threading
import time
from typing import List, Tuple

from .config import ChannelSettings
from .delivery import DeliveryTracker
from .fields import Field


class QueuedEvent:
    """A raw event handed from the emitter to the worker. The worker does the
    merge, hash, and blacklist work — the emit path stays cheap.
    """

    __slots__ = ("seq", "delivery_ticket", "schema_name", "event_fields", "state")

    def __init__(
        self,
        seq: int,
        delivery_ticket: int,
        schema_name: str,
        event_fields: List[Field],
        state: Tuple[Field, ...],
    ) -> None:
        #: Raw per-channel sequence assigned at emit. The worker subtracts the
        #: running blacklist-drop count to produce the wire sequence, so blacklist
        #: drops close the gap while queue-full drops leave one.
        self.seq = seq
        self.delivery_ticket = delivery_ticket
        self.schema_name = schema_name
        self.event_fields = event_fields
        self.state = state


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
    """The emitter-side handle for one channel."""

    __slots__ = ("settings", "queue", "_seq", "_rate", "_delivery")

    def __init__(self, settings: ChannelSettings, delivery: DeliveryTracker) -> None:
        self.settings = settings
        self.queue: "queue.Queue[QueuedEvent]" = queue.Queue(
            maxsize=max(1, settings.queue_capacity)
        )
        self._seq = itertools.count()  # next() is atomic under CPython
        self._delivery = delivery
        self._rate = (
            _TokenBucket(settings.max_events_per_second)
            if settings.max_events_per_second > 0
            else None
        )

    def try_emit(
        self, schema_name: str, event_fields: List[Field], state: Tuple[Field, ...]
    ) -> bool:
        """Attempt to emit a raw event. Returns ``False`` if rate-limited or the
        queue is full (both are non-blocking drops).

        Mirrors the other clients: rate-limit drops happen before a sequence
        number is consumed (no gap); queue-full drops happen after (leaving a gap
        that signals data loss).
        """
        ticket = self._delivery.begin()
        if self._rate is not None and not self._rate.try_take():
            self._delivery.reject(ticket)
            return False
        seq = next(self._seq)
        event = QueuedEvent(seq, ticket, schema_name, event_fields, state)
        try:
            self.queue.put_nowait(event)
            return True
        except queue.Full:
            self._delivery.reject(ticket)
            return False

    def emit_wait(
        self,
        schema_name: str,
        event_fields: List[Field],
        state: Tuple[Field, ...],
        timeout: float,
    ) -> bool:
        """Wait up to ``timeout`` seconds for queue capacity."""
        deadline = time.monotonic() + max(0.0, timeout)
        ticket = self._delivery.begin()
        if self._rate is not None:
            while not self._rate.try_take():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._delivery.reject(ticket)
                    return False
                time.sleep(min(0.01, remaining))

        seq = next(self._seq)
        event = QueuedEvent(seq, ticket, schema_name, event_fields, state)
        try:
            self.queue.put(event, timeout=max(0.0, deadline - time.monotonic()))
            return True
        except queue.Full:
            self._delivery.reject(ticket)
            return False

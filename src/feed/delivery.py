"""Constant-space delivery counters split at the current flush watermark."""

from __future__ import annotations

import threading
import time
from collections import Counter
from dataclasses import dataclass

DELIVERED = "delivered"
FILTERED = "filtered"
DROPPED = "dropped"


@dataclass(frozen=True)
class DeliveryReport:
    accepted: int
    delivered: int
    filtered: int
    dropped: int
    pending: int
    complete: bool
    timed_out: bool
    persisted_pending: int = 0
    unsaved: int = 0
    spool_path: str = ""
    storage_error: str = ""

    @property
    def successful(self):
        return self.complete and self.dropped == 0 and not self.storage_error

    @property
    def failed(self):
        """Rejected uploads retained on disk; ``dropped`` is the legacy name."""
        return self.dropped


class DeliveryTracker:
    """The worker settles each owned record once; no per-event history remains.

    Client serializes flush calls. A watermark separates current counts from
    later admissions, including uploads that finish out of order.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._next_ticket = 0
        self._boundary = None
        self._current, self._later = Counter(), Counter()
        self.spool_path = ""
        self.storage_error = ""

    def _counts(self, ticket):
        return (
            self._later
            if self._boundary is not None and ticket > self._boundary
            else self._current
        )

    def begin(self):
        with self._condition:
            ticket = self._next_ticket
            self._next_ticket += 1
            counts = self._counts(ticket)
            counts["accepted"] += 1
            counts["unsaved"] += 1
            return ticket

    def reject(self, ticket):
        with self._condition:
            counts = self._counts(ticket)
            counts["accepted"] -= 1
            counts["unsaved"] -= 1
            self._condition.notify_all()

    def persisted(self, ticket):
        with self._condition:
            self._counts(ticket)["unsaved"] -= 1
            self._condition.notify_all()

    def settle(self, tickets, outcome):
        with self._condition:
            for ticket in tickets:
                self._counts(ticket)[outcome] += 1
            self._condition.notify_all()

    def watermark(self):
        with self._condition:
            self._current.update(self._later)
            self._later.clear()
            self._boundary = self._next_ticket - 1
            return self._boundary

    def _pending(self):
        c = self._current
        return c["accepted"] - c[DELIVERED] - c[FILTERED] - c[DROPPED]

    def wait(self, watermark, timeout):
        deadline = time.monotonic() + max(0, timeout)
        with self._condition:
            while self._pending():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)
            c = self._current
            pending = self._pending()
            report = DeliveryReport(
                c["accepted"],
                c[DELIVERED],
                c[FILTERED],
                c[DROPPED],
                pending,
                pending == 0,
                pending != 0,
                pending - c["unsaved"],
                c["unsaved"],
                self.spool_path,
                self.storage_error,
            )
            if not pending:
                self._current, self._later = self._later, Counter()
                self._boundary = None
            return report

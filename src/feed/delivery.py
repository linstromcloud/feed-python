"""Delivery tracking for acknowledged flushes.

Delivery tickets are local-only identifiers. They let callers wait for events
accepted before a flush without changing their remote identity.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Set


DELIVERED = "delivered"
FILTERED = "filtered"
DROPPED = "dropped"


@dataclass(frozen=True)
class DeliveryReport:
    """Outcome for events accepted since the previous completed flush.

    ``complete`` means that every accepted event covered by this flush has
    reached a terminal outcome. ``successful`` additionally requires that no
    event was dropped. Events filtered by an explicit server rule are terminal
    but are reported separately.
    """

    accepted: int
    delivered: int
    filtered: int
    dropped: int
    pending: int
    complete: bool
    timed_out: bool

    @property
    def successful(self) -> bool:
        return self.complete and self.dropped == 0


class DeliveryTracker:
    """Thread-safe accepted-event tracker shared by emitters and the worker."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._next_ticket = 0
        self._pending: Set[int] = set()
        self._outcomes: Dict[int, str] = {}
        self._reported_through = -1

    def begin(self) -> int:
        """Reserve a ticket before attempting to place an event in a queue."""
        with self._condition:
            ticket = self._next_ticket
            self._next_ticket += 1
            self._pending.add(ticket)
            return ticket

    def reject(self, ticket: int) -> None:
        """Discard a ticket for an event that was never accepted by a queue."""
        with self._condition:
            self._pending.discard(ticket)
            self._condition.notify_all()

    def settle(self, tickets: Iterable[int], outcome: str) -> None:
        """Mark accepted tickets delivered, filtered, or permanently dropped."""
        with self._condition:
            for ticket in tickets:
                if ticket not in self._pending:
                    continue
                self._pending.remove(ticket)
                self._outcomes[ticket] = outcome
            self._condition.notify_all()

    def watermark(self) -> int:
        with self._condition:
            return self._next_ticket - 1

    def wait(self, watermark: int, timeout: float) -> DeliveryReport:
        """Wait for all accepted tickets through ``watermark`` to settle."""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._condition:
            while self._has_pending_through(watermark):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(remaining)

            pending = sum(
                1
                for ticket in self._pending
                if self._reported_through < ticket <= watermark
            )
            outcomes = [
                outcome
                for ticket, outcome in self._outcomes.items()
                if self._reported_through < ticket <= watermark
            ]
            complete = pending == 0
            report = DeliveryReport(
                accepted=len(outcomes) + pending,
                delivered=outcomes.count(DELIVERED),
                filtered=outcomes.count(FILTERED),
                dropped=outcomes.count(DROPPED),
                pending=pending,
                complete=complete,
                timed_out=not complete,
            )

            if complete and watermark > self._reported_through:
                for ticket in list(self._outcomes):
                    if ticket <= watermark:
                        del self._outcomes[ticket]
                self._reported_through = watermark

            return report

    def _has_pending_through(self, watermark: int) -> bool:
        return any(ticket <= watermark for ticket in self._pending)

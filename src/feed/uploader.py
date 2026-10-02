"""Batch preparation and outcomes shared by live delivery and feed sync."""

import json
import logging
import time
from collections import Counter
from dataclasses import dataclass, field

from .blacklist import Blacklist
from .config import ChannelSettings
from .delivery import DELIVERED, DROPPED, FILTERED
from .transport import Transport, backoff
from .spool import MAX_RECORD_BYTES

logger = logging.getLogger("feed")


@dataclass
class Batch:
    channel: str
    tickets: list
    events: list = field(default_factory=list)
    attempts: int = 0
    ready_at: float = 0
    inflight: bool = False


class UploadRun:
    def __init__(self, spool, config, delivery=None, once=False, transport=None):
        self.spool, self.config, self.delivery = spool, config, delivery
        self.transport = transport or Transport(config)
        self.once = once
        self.blacklist = Blacklist()
        self.channels = [
            ChannelSettings(**settings)
            for settings in spool.destination.get("channels", [{"name": "default"}])
        ]
        self.batches = []
        self.transitions = {}
        self.totals = Counter()
        self.last_upload = {c.name: time.monotonic() for c in self.channels}
        self.cursor = {c.name: -1 for c in self.channels}
        self.ready = False
        self.fetching = False
        self.fetch_after = 0
        self.fetch_attempts = 0
        self.force = delivery is None
        self.last_error = ""

    def error(self, reason):
        if reason != self.last_error:
            logger.warning(
                "[feed] %s; pending records retained in %s", reason, self.spool.path
            )
        self.last_error = reason

    def next_batch(self, channel):
        now = time.monotonic()
        batches = [b for b in self.batches if b.channel == channel.name]
        if sum(b.inflight for b in batches) >= max(1, channel.max_concurrent_slots):
            return None
        batch = next((b for b in batches if not b.inflight and b.ready_at <= now), None)
        if batch is None:
            # Waiting retries occupy bounded batch slots, while their payloads
            # remain on disk. One delayed event leaves other slots available.
            if len(batches) >= max(1, channel.max_concurrent_slots):
                return None
            held = {ticket for b in self.batches for ticket in b.tickets}
            limit = max(1, channel.flush_threshold_events)
            tickets = self.spool.ready(
                limit,
                held | self.transitions.keys(),
                channel=channel.name,
                after=self.cursor[channel.name],
            )
            if not tickets or (
                not self.force
                and len(tickets) < limit
                and now - self.last_upload[channel.name]
                < channel.flush_interval_seconds
            ):
                return None
            batch = Batch(channel.name, tickets)
            self.batches.append(batch)
        self._load(batch)
        if not batch.events:
            self.batches.remove(batch)
            return None
        batch.inflight = True
        self.last_upload[channel.name] = now
        return batch

    def _load(self, batch):
        events, actions, processed, size = [], [], [], 0
        for ticket in batch.tickets:
            event_size = self.spool.size(ticket)
            if processed and size + event_size > self.config.upload_batch_bytes:
                break
            processed.append(ticket)
            size += event_size
        for ticket, payload in self.spool.read_batch(processed):
            event_size = self.spool.size(ticket)
            try:
                if len(payload) != event_size or event_size > MAX_RECORD_BYTES:
                    raise ValueError("invalid spool record size")
                event = json.loads(payload)
                if event["ticket"] != ticket or event["channel"] != batch.channel:
                    raise ValueError("event identity differs from spool filename")
                for key in ("seq", "schema_hash", "schema_def", "data"):
                    event[key]
                if not isinstance(event["seq"], int) or not isinstance(
                    event["data"], dict
                ):
                    raise ValueError("invalid event sequence or data")
                event["_spool_size"] = event_size
            except (ValueError, KeyError, TypeError) as exc:
                event = {
                    "ticket": ticket,
                    "channel": batch.channel,
                    "_spool_size": event_size,
                }
                reason = f"corrupt record: {exc}"
                actions.append((event, "failed", DROPPED, reason))
                self.error(reason)
                continue
            events.append(event)
        filtered = self.spool.prepare_batch(events, self.blacklist)
        for event in events:
            if event["ticket"] in filtered:
                actions.append((event, "ack", FILTERED, ""))
        events = [event for event in events if event["ticket"] not in filtered]
        for action in actions:
            self._transition(*action)
        if self.once and processed:
            self.cursor[batch.channel] = max(self.cursor[batch.channel], max(processed))
        batch.events = events
        batch.tickets = [event["ticket"] for event in events]

    def result(self, batch, outcome):
        batch.inflight = False
        if outcome.kind == "retry":
            batch.ready_at = time.monotonic() + max(
                backoff(self.config, batch.attempts), outcome.retry_after
            )
            batch.attempts += 1
            batch.events = []
            if self.once:
                self.batches.remove(batch)
            self.error(outcome.reason)
            return
        self.batches.remove(batch)
        if outcome.kind == "large" and len(batch.tickets) > 1:
            middle = len(batch.tickets) // 2
            self.batches.extend(
                (
                    Batch(batch.channel, batch.tickets[:middle]),
                    Batch(batch.channel, batch.tickets[middle:]),
                )
            )
            logger.info(
                "[feed] splitting oversized batch events=%d into %d and %d",
                len(batch.tickets),
                middle,
                len(batch.tickets) - middle,
            )
            return
        for event in batch.events:
            if outcome.kind == "success":
                result = FILTERED if event["ticket"] in outcome.filtered else DELIVERED
                self._transition(event, "ack", result)
            else:
                self._transition(event, "failed", DROPPED, outcome.reason)
        if outcome.kind == "success":
            self.blacklist.merge_rules(outcome.rules)
            if self.last_error:
                logger.info("[feed] delivery recovered for %s", self.spool.session_id)
                self.last_error = ""
        else:
            self.error(outcome.reason)

    def _transition(self, event, kind, outcome, reason=""):
        ticket = event["ticket"]
        self.transitions[ticket] = (
            kind,
            event["channel"],
            event["_spool_size"],
            reason,
        )
        self.totals[outcome] += 1
        if self.delivery is not None:
            self.delivery.settle([ticket], outcome)

    def apply_transitions(self):
        # Retrying file cleanup must not settle delivery counters a second time.
        items = list(self.transitions.items())
        for offset in range(0, len(items), self.spool.batch_limit):
            group = items[offset : offset + self.spool.batch_limit]
            acks = [
                (ticket, channel, size)
                for ticket, (kind, channel, size, _) in group
                if kind == "ack"
            ]
            failures = [
                (ticket, reason)
                for ticket, (kind, _, _, reason) in group
                if kind != "ack"
            ]
            self.spool.ack_batch(acks)
            for ticket, _, _ in acks:
                del self.transitions[ticket]
            self.spool.fail_batch(failures)
            for ticket, _ in failures:
                del self.transitions[ticket]
        self.spool.release_unused()

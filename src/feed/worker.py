"""The background worker.

A single orchestrator thread owns all per-channel queues and runs the full
pipeline: drain queues, merge state, hash schemas, filter against the blacklist,
batch by flush trigger, and hand uploads to a small thread pool. Uploads run
concurrently (bounded) and report back over a result queue. The worker never
touches the caller's thread and ``emit`` never blocks on it.
"""

from __future__ import annotations

import enum
import gzip
import logging
import queue
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

import requests

from .blacklist import Blacklist, BlacklistRule, parse_rules
from .channel import ChannelHandle
from .config import Config
from .delivery import DELIVERED, DROPPED, FILTERED, DeliveryReport, DeliveryTracker
from .protocol import ProcessedEvent, build_batch_json, build_event
from .state import merge

logger = logging.getLogger("feed")

_TICK_SECONDS = 0.1
_USER_AGENT = "feed-python/0.1.0"


class WorkerState(enum.Enum):
    INITIALIZING = "initializing"
    FETCHING_BLACKLIST = "fetching_blacklist"
    RUNNING = "running"
    FINISHED = "finished"


class _Batch:
    __slots__ = ("channel_idx", "priority", "events", "retry_count")

    def __init__(
        self,
        channel_idx: int,
        priority: int,
        events: List[ProcessedEvent],
        retry_count: int,
    ) -> None:
        self.channel_idx = channel_idx
        self.priority = priority
        self.events = events
        self.retry_count = retry_count


class _Outcome:
    """Result of an upload attempt."""

    SUCCESS = "success"
    DROP = "drop"
    TOO_LARGE = "too_large"
    RETRY = "retry"

    __slots__ = ("kind", "rules", "events", "reason")

    def __init__(self, kind: str, rules=None, events=None, reason: str = "") -> None:
        self.kind = kind
        self.rules: List[BlacklistRule] = rules or []
        self.events: List[ProcessedEvent] = events or []
        self.reason = reason


class _BatchResult:
    __slots__ = ("channel_idx", "priority", "retry_count", "outcome")

    def __init__(
        self, channel_idx: int, priority: int, retry_count: int, outcome: _Outcome
    ) -> None:
        self.channel_idx = channel_idx
        self.priority = priority
        self.retry_count = retry_count
        self.outcome = outcome


class _ChannelState:
    __slots__ = (
        "settings",
        "wire_name",
        "queue",
        "blacklist_dropped",
        "current",
        "last_flush",
        "inflight",
    )

    def __init__(self, handle: ChannelHandle) -> None:
        self.settings = handle.settings
        self.wire_name = handle.settings.name.lower()
        self.queue = handle.queue
        self.blacklist_dropped = 0
        self.current: List[ProcessedEvent] = []
        self.last_flush = time.monotonic()
        self.inflight = 0


class Worker:
    def __init__(
        self,
        config: Config,
        session_id: str,
        handles: List[ChannelHandle],
        delivery: DeliveryTracker,
    ) -> None:
        self._config = config
        self._session_id = session_id
        self._channels = [_ChannelState(h) for h in handles]
        self._blacklist = Blacklist()
        self._delivery = delivery
        self._state = WorkerState.INITIALIZING

        self._stop = threading.Event()
        self._flush_requested = threading.Event()
        self._shutdown_flush_timeout = 10.0
        self._result_queue: "queue.Queue[_BatchResult]" = queue.Queue()
        self._ready: List[_Batch] = []
        self._retry_queue: List[Tuple[float, _Batch]] = []
        self._global_inflight = 0
        self._shutting_down = False

        self._thread_local = threading.local()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, config.max_concurrent_requests),
            thread_name_prefix="feed-upload",
        )
        self._thread = threading.Thread(
            target=self._run, name="feed-worker", daemon=True
        )

    # --- lifecycle --------------------------------------------------------

    def start(self) -> None:
        self._thread.start()

    @property
    def state(self) -> WorkerState:
        return self._state

    def flush(self, timeout: float) -> DeliveryReport:
        """Force partial batches out and wait for the current delivery watermark."""
        watermark = self._delivery.watermark()
        self._flush_requested.set()
        return self._delivery.wait(watermark, timeout)

    def shutdown(self, flush_timeout: float) -> DeliveryReport:
        """Signal the worker to flush and stop, then wait for it to finish."""
        watermark = self._delivery.watermark()
        started = time.monotonic()
        if self._stop.is_set():
            self._thread.join(timeout=flush_timeout + 5.0)
            remaining = max(0.0, flush_timeout - (time.monotonic() - started))
            return self._delivery.wait(watermark, remaining)
        self._shutdown_flush_timeout = flush_timeout
        self._stop.set()
        self._thread.join(timeout=flush_timeout + 5.0)
        remaining = max(0.0, flush_timeout - (time.monotonic() - started))
        return self._delivery.wait(watermark, remaining)

    # --- main loop --------------------------------------------------------

    def _run(self) -> None:
        try:
            self._state = WorkerState.FETCHING_BLACKLIST
            if not self._fetch_blacklist():
                # 404, abandoned, or stopped during fetch: telemetry disabled.
                return
            self._state = WorkerState.RUNNING

            while not self._stop.is_set():
                force = self._flush_requested.is_set()
                self._drain_and_batch(force=force)
                if force:
                    self._flush_requested.clear()
                self._promote_due_retries()
                self._dispatch_ready()
                self._poll_results(_TICK_SECONDS)

            self._graceful_shutdown()
        finally:
            self._executor.shutdown(wait=False)
            self._state = WorkerState.FINISHED

    def _graceful_shutdown(self) -> None:
        self._shutting_down = True
        self._drain_and_batch(force=True)
        self._promote_due_retries()
        deadline = time.monotonic() + self._shutdown_flush_timeout
        while (
            self._ready or self._global_inflight > 0
        ) and time.monotonic() < deadline:
            self._dispatch_ready()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            self._poll_results(remaining)
            # Move any just-scheduled retries forward immediately (no backoff wait).
            for _, batch in self._retry_queue:
                self._ready.append(batch)
            self._retry_queue.clear()

    # --- blacklist fetch --------------------------------------------------

    def _fetch_blacklist(self) -> bool:
        url = f"{self._config.server_url.rstrip('/')}/v1/{self._config.endpoint_id}/blacklist"
        attempt = 0
        while not self._stop.is_set():
            try:
                resp = self._session().get(
                    url,
                    headers=self._headers(),
                    timeout=self._config.blacklist_timeout_seconds,
                )
                if resp.status_code == 200:
                    rules = parse_rules(resp.json().get("rules"))
                    self._blacklist.set_rules(rules)
                    logger.debug(
                        "feed: blacklist ready (%d rules)", len(self._blacklist)
                    )
                    return True
                if resp.status_code == 404:
                    logger.error(
                        "feed: rules endpoint 404 (unknown endpoint_id '%s'); ingestion disabled",
                        self._config.endpoint_id,
                    )
                    return False
                logger.warning("feed: blacklist fetch HTTP %d", resp.status_code)
            except Exception as exc:  # noqa: BLE001 - best-effort telemetry
                logger.warning("feed: blacklist fetch error: %s", exc)

            attempt += 1
            if (
                self._config.max_blacklist_fetch_attempts != 0
                and attempt >= self._config.max_blacklist_fetch_attempts
            ):
                logger.error(
                    "feed: blacklist fetch abandoned; telemetry disabled for session"
                )
                return False
            if self._stop.wait(self._backoff_delay(attempt - 1)):
                return False
        return False

    # --- batching ---------------------------------------------------------

    def _drain_and_batch(self, force: bool) -> None:
        now = time.monotonic()
        for idx, ch in enumerate(self._channels):
            while True:
                try:
                    ev = ch.queue.get_nowait()
                except queue.Empty:
                    break
                merged = merge(ev.state, ev.event_fields)
                schema_hash, schema_def, data = build_event(ev.schema_name, merged)
                if self._blacklist.is_blacklisted(schema_hash, data):
                    ch.blacklist_dropped += 1
                    self._delivery.settle([ev.delivery_ticket], FILTERED)
                    continue
                wire_seq = ev.seq - ch.blacklist_dropped
                ch.current.append(
                    ProcessedEvent(
                        wire_seq,
                        ev.delivery_ticket,
                        ch.wire_name,
                        schema_hash,
                        schema_def,
                        data,
                    )
                )

            threshold = max(1, ch.settings.flush_threshold_events)
            while len(ch.current) >= threshold:
                events = ch.current[:threshold]
                ch.current = ch.current[threshold:]
                self._ready.append(_Batch(idx, ch.settings.priority, events, 0))
                ch.last_flush = now

            due = (now - ch.last_flush) >= ch.settings.flush_interval_seconds
            if ch.current and (force or due):
                self._ready.append(_Batch(idx, ch.settings.priority, ch.current, 0))
                ch.current = []
                ch.last_flush = now

    def _promote_due_retries(self) -> None:
        now = time.monotonic()
        still_waiting = []
        for when, batch in self._retry_queue:
            if when <= now:
                self._ready.append(batch)
            else:
                still_waiting.append((when, batch))
        self._retry_queue = still_waiting

    def _dispatch_ready(self) -> None:
        self._ready.sort(key=lambda b: b.priority)
        max_requests = max(1, self._config.max_concurrent_requests)
        remaining: List[_Batch] = []
        for batch in self._ready:
            ch = self._channels[batch.channel_idx]
            if self._global_inflight >= max_requests or ch.inflight >= max(
                1, ch.settings.max_concurrent_slots
            ):
                remaining.append(batch)
                continue
            self._global_inflight += 1
            ch.inflight += 1
            self._executor.submit(self._upload_task, batch)
        self._ready = remaining

    # --- upload (runs on pool threads) -----------------------------------

    def _upload_task(self, batch: _Batch) -> None:
        outcome = self._upload_once(batch.events)
        self._result_queue.put(
            _BatchResult(batch.channel_idx, batch.priority, batch.retry_count, outcome)
        )

    def _upload_once(self, events: List[ProcessedEvent]) -> _Outcome:
        url = f"{self._config.server_url.rstrip('/')}/v1/{self._config.endpoint_id}/telemetry"
        batch_json = build_batch_json(self._session_id, events)
        body = gzip.compress(batch_json)
        logger.debug(
            "feed: uploading batch events=%d json_bytes=%d gzip_bytes=%d",
            len(events),
            len(batch_json),
            len(body),
        )
        try:
            headers = self._headers()
            headers["Content-Type"] = "application/json"
            headers["Content-Encoding"] = "gzip"
            resp = self._session().post(
                url,
                data=body,
                headers=headers,
                timeout=self._config.upload_timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            return _Outcome(_Outcome.RETRY, events=events, reason=str(exc))

        status = resp.status_code
        if 200 <= status < 300:
            rules: List[BlacklistRule] = []
            try:
                rules = parse_rules(resp.json().get("blacklisted"))
            except Exception:  # noqa: BLE001 - body optional/garbled
                pass
            return _Outcome(_Outcome.SUCCESS, rules=rules, events=events)
        if status == 413:
            logger.info(
                "feed: server returned 413 for batch events=%d json_bytes=%d gzip_bytes=%d",
                len(events),
                len(batch_json),
                len(body),
            )
            return _Outcome(_Outcome.TOO_LARGE, events=events)
        if status in (408, 429, 503) or 500 <= status < 600:
            return _Outcome(_Outcome.RETRY, events=events, reason=f"HTTP {status}")
        return _Outcome(_Outcome.DROP, events=events, reason=f"HTTP {status}")

    # --- result handling (runs on worker thread) -------------------------

    def _poll_results(self, timeout: float) -> None:
        try:
            first = self._result_queue.get(timeout=max(0.0, timeout))
        except queue.Empty:
            return
        self._handle_result(first)
        while True:
            try:
                self._handle_result(self._result_queue.get_nowait())
            except queue.Empty:
                break

    def _handle_result(self, result: _BatchResult) -> None:
        self._global_inflight = max(0, self._global_inflight - 1)
        ch = self._channels[result.channel_idx]
        ch.inflight = max(0, ch.inflight - 1)
        outcome = result.outcome

        if outcome.kind == _Outcome.SUCCESS:
            self._delivery.settle(
                [event.delivery_ticket for event in outcome.events], DELIVERED
            )
            if outcome.rules:
                self._blacklist.merge_rules(outcome.rules)
        elif outcome.kind == _Outcome.DROP:
            logger.warning("feed: dropping batch (no retry): %s", outcome.reason)
            self._delivery.settle(
                [event.delivery_ticket for event in outcome.events], DROPPED
            )
        elif outcome.kind == _Outcome.TOO_LARGE:
            events = outcome.events
            if len(events) <= 1:
                logger.warning("feed: single event exceeds max payload; dropping")
                self._delivery.settle(
                    [event.delivery_ticket for event in events], DROPPED
                )
                return
            mid = len(events) // 2
            logger.info(
                "feed: splitting oversized batch events=%d into %d and %d",
                len(events),
                mid,
                len(events) - mid,
            )
            for half in (events[:mid], events[mid:]):
                self._retry_queue.append(
                    (
                        time.monotonic(),
                        _Batch(
                            result.channel_idx,
                            result.priority,
                            half,
                            result.retry_count,
                        ),
                    )
                )
            self._enforce_retry_depth()
        elif outcome.kind == _Outcome.RETRY:
            next_count = result.retry_count + 1
            if self._config.max_retries != 0 and next_count > self._config.max_retries:
                logger.warning(
                    "feed: dropping batch after %d retries: %s",
                    result.retry_count,
                    outcome.reason,
                )
                self._delivery.settle(
                    [event.delivery_ticket for event in outcome.events], DROPPED
                )
                return
            delay = (
                0.0 if self._shutting_down else self._backoff_delay(result.retry_count)
            )
            self._retry_queue.append(
                (
                    time.monotonic() + delay,
                    _Batch(
                        result.channel_idx, result.priority, outcome.events, next_count
                    ),
                )
            )
            self._enforce_retry_depth()

    def _enforce_retry_depth(self) -> None:
        if self._config.max_retry_queue_depth == 0:
            return
        max_depth = max(1, self._config.max_retry_queue_depth)
        while len(self._retry_queue) > max_depth:
            _, batch = self._retry_queue.pop(0)
            self._delivery.settle(
                [event.delivery_ticket for event in batch.events], DROPPED
            )
            logger.warning("feed: retry queue full, dropped oldest batch")

    # --- helpers ----------------------------------------------------------

    def _backoff_delay(self, attempt: int) -> float:
        base = self._config.retry_base_delay_seconds
        cap = self._config.retry_max_delay_seconds
        capped = min(base * (2 ** min(attempt, 30)), cap)
        return capped * random.uniform(0.5, 1.5)

    def _headers(self) -> Dict[str, str]:
        headers = {"User-Agent": _USER_AGENT}
        if self._config.bearer_token_provider:
            headers["Authorization"] = f"Bearer {self._config.bearer_token_provider()}"
        elif self._config.client_secret:
            headers["X-Client-Secret"] = self._config.client_secret
        return headers

    def _session(self) -> requests.Session:
        s: Optional[requests.Session] = getattr(self._thread_local, "session", None)
        if s is None:
            s = requests.Session()
            self._thread_local.session = s
        return s

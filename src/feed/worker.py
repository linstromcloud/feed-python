"""Background persistence and bounded, channel-prioritized HTTP delivery."""

from __future__ import annotations

from collections import deque
import enum
import logging
import queue
import threading
import time

from .uploader import UploadRun
from .transport import Outcome, Transport, backoff

logger = logging.getLogger("feed")


class WorkerState(enum.Enum):
    INITIALIZING = "initializing"
    FETCHING_BLACKLIST = "fetching_blacklist"
    RUNNING = "running"
    FINISHED = "finished"


class Worker:
    def __init__(self, config, session_id, handles, delivery, spool, admission, wake):
        self._config, self._handles, self._delivery = config, handles, delivery
        self._spool, self._admission, self._wake = spool, admission, wake
        self._transport = Transport(config)
        self._current = UploadRun(spool, config, delivery, transport=self._transport)
        self._contexts = [self._current]
        self._recoveries = iter(spool.root.runs(spool.destination))
        self._state = WorkerState.INITIALIZING
        self._stop = threading.Event()
        self._finished = threading.Event()
        self._announce_finish = False
        self._flush_requested = threading.Event()
        self._deadline = float("inf")
        self._results = queue.Queue()
        self._jobs = 0
        self._last_persist = time.monotonic()
        self._pending_writes = []
        self._quota_blocked = False
        self._requests = queue.Queue()
        self._http_threads = []
        self._storage_errors = {}
        self._thread = threading.Thread(
            target=self._run, name="feed-worker", daemon=True
        )

    def start(self):
        self._thread.start()

    @property
    def state(self):
        return self._state

    def flush(self, timeout):
        watermark = self._delivery.watermark()
        self._flush_requested.set()
        self._wake.set()
        return self._delivery.wait(watermark, timeout)

    def shutdown(self, flush_timeout, *, announce=False):
        self._admission.stop()
        watermark = self._delivery.watermark()
        deadline = (
            float("inf")
            if flush_timeout is None
            else time.monotonic() + max(0, flush_timeout)
        )
        self._deadline = min(self._deadline, deadline)
        self._announce_finish = announce
        self._stop.set()
        self._wake.set()
        try:
            while not self._finished.is_set():
                remaining = deadline + 0.1 - time.monotonic()
                if remaining <= 0:
                    break
                self._finished.wait(min(0.1, remaining))
        except KeyboardInterrupt:
            self._deadline = time.monotonic()
            self._wake.set()
            raise
        return self._delivery.wait(watermark, 0)

    def _submit(self, context, kind, batch=None):
        self._jobs += 1
        if self._jobs > len(self._http_threads):
            thread = threading.Thread(
                target=self._http_loop, name="feed-upload", daemon=True
            )
            thread.start()
            self._http_threads.append(thread)
        if kind == "blacklist":
            context.fetching = True
        self._requests.put((context, kind, batch))

    def _http_loop(self):
        # Persistent daemon workers reuse their own Session and never touch
        # spool files. Shutdown does not wait past its deadline for HTTP.
        try:
            while True:
                request = self._requests.get()
                if request is None:
                    return
                context, kind, batch = request
                try:
                    result = (
                        context.transport.blacklist()
                        if kind == "blacklist"
                        else context.transport.upload(
                            context.spool.session_id, batch.events
                        )
                    )
                except Exception as exc:
                    result = Outcome("retry", str(exc))
                self._results.put((context, kind, batch, result))
                self._wake.set()
        finally:
            self._transport.close()

    def _run(self):
        try:
            self._state = WorkerState.FETCHING_BLACKLIST
            while True:
                request = self._spool.path / "flush.request"
                if request.exists():
                    request.unlink()
                    self._flush_requested.set()
                force = self._flush_requested.is_set() or self._stop.is_set()
                if force:
                    self._current.force = True
                    self._flush_requested.clear()
                self._persist(force)
                self._poll_results()
                self._apply_transitions()
                if self._stop.is_set() and time.monotonic() >= self._deadline:
                    break
                self._adopt_recoveries()
                if self._stop.is_set():
                    pending = self._admission.used or any(
                        c.transitions or c.spool.counts()["pending"]
                        for c in self._contexts
                    )
                    if not pending and self._jobs == 0:
                        break
                    if pending and self._announce_finish:
                        print("[feed] Syncing remaining feeds, cancel with Ctrl+C.", flush=True)
                        self._announce_finish = False
                try:
                    self._dispatch()
                    self._clear_storage_error("dispatch")
                except OSError as exc:
                    self._storage_error(exc, "dispatch")
                self._wake.wait(0.05)
                self._wake.clear()
        except Exception as exc:
            self._storage_error(exc)
            logger.exception("[feed] worker stopped; durable records remain recoverable")
        finally:
            try:
                self._admission.stop()
                for _ in self._http_threads:
                    self._requests.put(None)
                for context in self._contexts:
                    try:
                        context.spool.close()
                    except OSError as exc:
                        self._storage_error(exc)
            finally:
                self._state = WorkerState.FINISHED
                self._finished.set()

    def _storage_error(self, exc, key="worker"):
        reason = str(exc)
        self._storage_errors[key] = reason
        if reason != self._delivery.storage_error:
            logger.error("[feed] persistence error: %s", reason)
        self._delivery.storage_error = reason
        self._admission.storage_error(reason)

    def _clear_storage_error(self, key):
        self._storage_errors.pop(key, None)
        reason = next(iter(self._storage_errors.values()), "")
        self._delivery.storage_error = reason
        self._admission.storage_error(reason)

    def _persist(self, force):
        now = time.monotonic()
        due = now - self._last_persist >= self._config.persist_interval_seconds
        full = self._admission.used >= self._config.persist_threshold_bytes
        channel_due = any(
            h.queue.qsize() >= max(1, h.settings.flush_threshold_events)
            for h in self._handles
        )
        if not (force or due or full or channel_due or self._pending_writes):
            return
        for handle in sorted(self._handles, key=lambda h: h.settings.priority):
            size = 0
            while size < self._config.persist_threshold_bytes:
                try:
                    event = handle.queue.get_nowait()
                except queue.Empty:
                    break
                self._pending_writes.append(event)
                size += len(event.payload)
        try:
            needed = sum(
                self._spool.cost(len(e.payload))
                for e in self._pending_writes
                if not e.reserved
            )
            if needed:
                self._spool.replenish(needed)
            remaining = []
            pending = deque(self._pending_writes)
            blocked_channels = set()
            while pending:
                if self._stop.is_set() and time.monotonic() >= self._deadline:
                    remaining.extend(pending)
                    break
                channel = pending[0].channel
                if channel in blocked_channels:
                    remaining.append(pending.popleft())
                    continue
                group, size = [], 0
                while (
                    pending
                    and pending[0].channel == channel
                    and len(group) < self._spool.batch_limit
                ):
                    event = pending[0]
                    if group and size + len(event.payload) > min(
                        self._config.persist_threshold_bytes, self._spool.batch_bytes
                    ):
                        break
                    if not event.reserved:
                        event.reserved = self._spool.reserve(len(event.payload))
                    if not event.reserved:
                        blocked_channels.add(channel)
                        break
                    group.append(pending.popleft())
                    size += len(event.payload)
                if not group:
                    continue
                try:
                    count = self._spool.persist_batch(
                        [
                            (event.delivery_ticket, event.payload, event.channel)
                            for event in group
                        ]
                    )
                except OSError:
                    self._pending_writes = remaining + group + list(pending)
                    raise
                for event in group[:count]:
                    self._delivery.persisted(event.delivery_ticket)
                    self._admission.release(len(event.payload))
                pending.extendleft(reversed(group[count:]))
            self._pending_writes = remaining
            self._spool.release_unused()
            blocked = any(not e.reserved for e in remaining)
            if blocked and not self._quota_blocked:
                logger.warning(
                    "[feed] spool quota exhausted; queued records remain in bounded memory"
                )
            self._quota_blocked = blocked
            self._last_persist = now
            self._clear_storage_error("persist")
        except OSError as exc:
            self._storage_error(exc, "persist")

    def _adopt_recoveries(self):
        for context in list(self._contexts):
            if (
                context is not self._current
                and not context.batches
                and not context.fetching
                and not context.transitions
            ):
                if context.spool.counts()["pending"] == 0:
                    context.spool.close()
                    self._contexts.remove(context)
        while len(self._contexts) < max(2, self._config.max_concurrent_requests + 1):
            try:
                path = next(self._recoveries)
            except StopIteration:
                break
            if path == self._spool.path:
                continue
            recovered = None
            try:
                recovered = self._spool.root.claim(path)
                if recovered is None:
                    continue
                if recovered.counts()["pending"]:
                    self._contexts.append(
                        UploadRun(recovered, self._config, transport=self._transport)
                    )
                    recovered = None
                else:
                    recovered.close()
                    recovered = None
            except (OSError, ValueError, TypeError, KeyError) as exc:
                logger.error("[feed] cannot recover %s: %s", path, exc)
            finally:
                if recovered is not None:
                    recovered.close()

    def _dispatch(self):
        maximum = max(1, self._config.max_concurrent_requests)
        # Fetching policy occupies an HTTP slot, never the persistence thread.
        for context in self._contexts:
            if self._jobs >= maximum:
                return
            if (
                not context.ready
                and not context.fetching
                and not context.transitions
                and time.monotonic() >= context.fetch_after
            ):
                self._submit(context, "blacklist")
        choices = [
            (channel.priority, index, channel)
            for index, context in enumerate(self._contexts)
            if context.ready and not context.transitions
            for channel in context.channels
        ]
        for _, index, channel in sorted(
            choices, key=lambda choice: (choice[0], choice[1])
        ):
            context = self._contexts[index]
            while self._jobs < maximum:
                batch = context.next_batch(channel)
                if batch is None:
                    break
                self._submit(context, "upload", batch)

    def _poll_results(self):
        while True:
            try:
                context, kind, batch, outcome = self._results.get_nowait()
            except queue.Empty:
                break
            self._jobs -= 1
            if kind == "blacklist":
                context.fetching = False
                if outcome.kind == "success":
                    context.ready = True
                    context.blacklist.set_rules(outcome.rules)
                    if context is self._current:
                        self._state = WorkerState.RUNNING
                else:
                    context.error(outcome.reason)
                    context.fetch_after = time.monotonic() + max(
                        backoff(self._config, context.fetch_attempts),
                        outcome.retry_after,
                    )
                    context.fetch_attempts += 1
                continue
            context.result(batch, outcome)

    def _apply_transitions(self):
        for context in self._contexts:
            key = "cleanup:" + context.spool.session_id
            try:
                context.apply_transitions()
                if (
                    context is self._current
                    and self._admission.used == 0
                    and not context.batches
                    and not context.spool.counts()["pending"]
                ):
                    context.force = False
                self._clear_storage_error(key)
            except OSError as exc:
                self._storage_error(exc, key)

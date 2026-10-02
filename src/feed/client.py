"""Typed events, session state, dictionary logging, and background delivery."""

from __future__ import annotations

import uuid
import threading
import hashlib
import logging
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Union, overload

from .channel import ChannelHandle
from .config import DEFAULT_CHANNEL, ChannelSettings, Config
from .delivery import DeliveryReport, DeliveryTracker
from .errors import ConfigError
from .fields import EventBuilder, Field, FieldType, _infer_field, _normalize_name
from .state import StateStore
from .limits import Admission
from .spool import SpoolRoot, MAX_RECORD_BYTES
from .worker import Worker, WorkerState


@dataclass(frozen=True)
class Channel:
    """An opaque handle to a channel, returned by :meth:`Client.channel`."""

    index: int


class Client:
    """A Feed session with events, shared state, and delivery tracking.

    Construct with :class:`Config` for direct endpoint configuration, or use
    :func:`feed.init` for a saved login or API key.
    """

    def __init__(self, config: Config) -> None:
        _validate(config)
        self._enabled = config.enabled
        self._session_id = str(uuid.uuid4())
        self._state = StateStore()
        self._delivery = DeliveryTracker()
        self._flush_lock = threading.Lock()
        self._config = config
        self._admission = None
        self._wake = threading.Event()

        # Channel names are lowercased so lookup/registration is case-insensitive.
        settings: List[ChannelSettings] = []
        for s in config.channels:
            settings.append(ChannelSettings(**{**s.__dict__, "name": s.name.lower()}))
        if not any(s.name == DEFAULT_CHANNEL for s in settings):
            settings.insert(0, ChannelSettings(DEFAULT_CHANNEL))

        spool = None
        if self._enabled:
            destination = {
                "server_url": config.server_url.rstrip("/"),
                "endpoint_id": config.endpoint_id,
                "project_id": config.project_id,
                "feed_id": config.feed_id,
                "control_url": config.control_url,
                "feed_reference": config.feed_reference,
                "auth": "member" if config.bearer_token_provider else "api_key",
                "key_id": hashlib.sha256(config.client_secret.encode()).hexdigest()
                if config.client_secret is not None
                else None,
                "channels": [s.__dict__ for s in settings],
            }
            if len(json.dumps(destination).encode()) + 256 * len(settings) > 7500:
                raise ConfigError(
                    "channel and destination metadata exceed the spool header budget"
                )
            root = SpoolRoot(config.spool_dir, config.max_spool_bytes)
            spool = root.create(destination, self._session_id, 0)
            self._admission = Admission(config.memory_queue_bytes, self._wake)
            self._delivery.spool_path = str(spool.path)
        self._handles: List[ChannelHandle] = [
            ChannelHandle(s, self._delivery, self._admission, config.max_event_bytes)
            for s in settings
        ]
        self._channel_index: Dict[str, int] = {
            s.name: i for i, s in enumerate(settings)
        }
        self._default_channel = self._channel_index[DEFAULT_CHANNEL]

        self._worker: Optional[Worker] = None
        if self._enabled:
            self._worker = Worker(
                config,
                self._session_id,
                self._handles,
                self._delivery,
                spool,
                self._admission,
                self._wake,
            )
            self._worker.start()
            print(f"[feed] Session: {self._session_id}", flush=True)

    # --- introspection ----------------------------------------------------

    @property
    def session_id(self) -> str:
        """The session UUID generated at construction."""
        return self._session_id

    @property
    def id(self) -> str:
        """The session UUID, also available as :attr:`session_id`."""
        return self._session_id

    @property
    def feed(self) -> str:
        """The selected feed reference, or the directly configured endpoint ID."""
        return self._config.feed_reference or self._config.endpoint_id

    @property
    def worker_state(self) -> WorkerState:
        """Current worker lifecycle state."""
        return self._worker.state if self._worker is not None else WorkerState.FINISHED

    @property
    def is_running(self) -> bool:
        """Whether ingestion is enabled and the worker has not finished."""
        return self._enabled and self.worker_state != WorkerState.FINISHED

    @property
    def enabled(self) -> bool:
        """Whether this client was configured to send data."""
        return self._enabled

    @property
    def max_event_bytes(self):
        return self._config.max_event_bytes

    # --- channels ---------------------------------------------------------

    def channel(self, name: str) -> Channel:
        """Look up a channel by name (case-insensitive). Unknown names fall back
        to the default channel so ``emit`` still works."""
        return Channel(self._channel_index.get(name.lower(), self._default_channel))

    # --- emit -------------------------------------------------------------

    def emit(self, schema_name: str, fields: List[Field]) -> bool:
        """Emit an event on the default channel. See :meth:`emit_on`."""
        return self.emit_on(Channel(self._default_channel), schema_name, fields)

    def emit_on(self, channel: Channel, schema_name: str, fields: List[Field]) -> bool:
        """Emit an event on a specific channel.

        Non-blocking. Returns ``False`` if ingestion is disabled, the worker has
        finished, the channel queue is full, or a rate limit rejected the event.
        A ``False`` from a full queue intentionally leaves a sequence-number gap
        so data loss is visible downstream.
        """
        if not self._enabled or self.worker_state == WorkerState.FINISHED:
            return False
        schema_name = _normalize_name(schema_name, "stream name")
        if not (0 <= channel.index < len(self._handles)):
            return False
        state = self._state.snapshot()
        return self._handles[channel.index].try_emit(schema_name, fields, state)

    def emit_wait(self, schema_name: str, fields: List[Field], timeout: float) -> bool:
        """Wait for default-channel queue capacity for up to ``timeout`` seconds."""
        return self.emit_on_wait(
            Channel(self._default_channel), schema_name, fields, timeout
        )

    def emit_on_wait(
        self,
        channel: Channel,
        schema_name: str,
        fields: List[Field],
        timeout: float,
    ) -> bool:
        """Bounded-wait counterpart to :meth:`emit_on`."""
        if not self._enabled or self.worker_state == WorkerState.FINISHED:
            return False
        schema_name = _normalize_name(schema_name, "stream name")
        if not (0 <= channel.index < len(self._handles)):
            return False
        state = self._state.snapshot()
        return self._handles[channel.index].emit_wait(
            schema_name, fields, state, timeout
        )

    # --- dictionary logging -----------------------------------------------

    @overload
    def log(self, record: Mapping[str, Any], /) -> bool: ...

    @overload
    def log(self, stream_name: str, record: Mapping[str, Any], /) -> bool: ...

    def log(
        self,
        stream_or_record: Union[str, Mapping[str, Any]],
        record: Optional[Mapping[str, Any]] = None,
        /,
    ) -> bool:
        """Append one native typed row to a stream.

        ``log(record)`` uses the default ``log`` stream;
        ``log(stream_name, record)`` selects a named stream.
        Each top-level key is a column. Values use inferred types, including
        typed structs for dictionaries.
        Returns ``False`` without inspecting the record when this client is
        disabled.
        """
        if isinstance(stream_or_record, str):
            if record is None:
                raise TypeError("log(stream_name, record) requires a record")
            return self._emit_record(stream_or_record, record, enqueue_timeout=None)
        if record is not None:
            raise TypeError("log(record) accepts only one record argument")
        return self._emit_record("log", stream_or_record, enqueue_timeout=None)

    @overload
    def log_wait(self, record: Mapping[str, Any], /, *, timeout: float) -> bool: ...

    @overload
    def log_wait(
        self,
        stream_name: str,
        record: Mapping[str, Any],
        /,
        *,
        timeout: float,
    ) -> bool: ...

    def log_wait(
        self,
        stream_or_record: Union[str, Mapping[str, Any]],
        record: Optional[Mapping[str, Any]] = None,
        /,
        *,
        timeout: float,
    ) -> bool:
        """Wait for queue capacity while appending one native typed row."""
        if isinstance(stream_or_record, str):
            if record is None:
                raise TypeError("log_wait(stream_name, record) requires a record")
            return self._emit_record(stream_or_record, record, enqueue_timeout=timeout)
        if record is not None:
            raise TypeError("log_wait(record) accepts only one record argument")
        return self._emit_record("log", stream_or_record, enqueue_timeout=timeout)

    def _emit_record(
        self,
        schema_name: str,
        data: Mapping[str, Any],
        *,
        enqueue_timeout: Optional[float],
    ) -> bool:
        if not self.enabled:
            return False
        if not isinstance(data, Mapping):
            raise TypeError("record must be a mapping")
        builder = EventBuilder()
        for field_name, value in data.items():
            builder.add(field_name, value)
        fields = builder.build()
        if enqueue_timeout is None:
            return self.emit(schema_name, fields)
        return self.emit_wait(schema_name, fields, enqueue_timeout)

    # --- state ------------------------------------------------------------

    def set_state(self, name: str, value) -> None:
        """Set a persistent field, inferring its type from the value."""
        f = _infer_field(name, value)
        self._state.set(f.name, f.ftype, f.value, f.type_descriptor())

    def set_state_bool(self, name: str, value: bool) -> None:
        self._state.set(name, FieldType.BOOL, bool(value))

    def set_state_int(self, name: str, value: int) -> None:
        self._state.set(name, FieldType.INT64, int(value))

    def set_state_float(self, name: str, value: float) -> None:
        self._state.set(name, FieldType.FLOAT64, float(value))

    def set_state_string(self, name: str, value: str) -> None:
        self._state.set(name, FieldType.STRING, str(value))

    def set_state_bool_array(self, name: str, values: List[bool]) -> None:
        self._state.set(name, FieldType.BOOL_ARRAY, [bool(v) for v in values])

    def set_state_int_array(self, name: str, values: List[int]) -> None:
        self._state.set(name, FieldType.INT64_ARRAY, [int(v) for v in values])

    def set_state_float_array(self, name: str, values: List[float]) -> None:
        self._state.set(name, FieldType.FLOAT64_ARRAY, [float(v) for v in values])

    def set_state_string_array(self, name: str, values: List[str]) -> None:
        self._state.set(name, FieldType.STRING_ARRAY, [str(v) for v in values])

    def set_state_optional_bool(self, name: str, value: Optional[bool]) -> None:
        self._state.set(
            name, FieldType.OPTIONAL_BOOL, None if value is None else bool(value)
        )

    def set_state_optional_int(self, name: str, value: Optional[int]) -> None:
        self._state.set(
            name, FieldType.OPTIONAL_INT64, None if value is None else int(value)
        )

    def set_state_optional_float(self, name: str, value: Optional[float]) -> None:
        self._state.set(
            name, FieldType.OPTIONAL_FLOAT64, None if value is None else float(value)
        )

    def set_state_optional_string(self, name: str, value: Optional[str]) -> None:
        self._state.set(
            name, FieldType.OPTIONAL_STRING, None if value is None else str(value)
        )

    def remove_state(self, name: str) -> None:
        """Remove a state field if present."""
        self._state.remove(name)

    def has_state(self, name: str) -> bool:
        """Whether a state field currently exists (case-insensitive)."""
        return self._state.has(name)

    # --- lifecycle --------------------------------------------------------

    def flush(self, timeout: float = 10.0) -> DeliveryReport:
        """Flush events accepted so far without stopping the worker."""
        if self._worker is None:
            return DeliveryReport(0, 0, 0, 0, 0, True, False)
        with self._flush_lock:
            return self._worker.flush(timeout)

    def shutdown(self, flush_timeout: float = 10.0) -> DeliveryReport:
        """Flush pending events and stop the worker, waiting up to
        ``flush_timeout`` seconds for in-flight uploads. Safe to call twice."""
        if self._worker is not None:
            with self._flush_lock:
                return self._worker.shutdown(flush_timeout)
        return DeliveryReport(0, 0, 0, 0, 0, True, False)

    def finish(self, timeout: Optional[float] = None) -> DeliveryReport:
        """Wait for pending delivery and stop the worker.

        An explicit timeout bounds the wait. Ctrl+C cancels it and leaves
        persisted records available for recovery.
        """
        if self._worker is None:
            return self.shutdown(0)
        with self._flush_lock:
            try:
                report = self._worker.shutdown(timeout, announce=True)
            except KeyboardInterrupt:
                self._print_finish_status(cancelled=True)
                raise
            self._print_finish_status()
        if not report.successful:
            logging.getLogger("feed").warning(
                "[feed] finish incomplete: persisted_pending=%d unsaved=%d failed=%d; spool=%s",
                report.persisted_pending,
                report.unsaved,
                report.failed,
                report.spool_path,
            )
        return report

    def _print_finish_status(self, *, cancelled=False):
        if self._worker is None:
            return
        report = self._delivery.snapshot()
        status = "cancelled" if cancelled else (
            "complete" if report.successful else "incomplete"
        )
        counts = [f"{report.delivered} delivered"]
        for count, label in (
            (report.filtered, "filtered"),
            (report.failed, "failed"),
            (report.persisted_pending, "pending on disk"),
            (report.unsaved, "unsaved"),
        ):
            if count:
                counts.append(f"{count} {label}")
        print(f"[feed] {status.capitalize()}: {', '.join(counts)}.", flush=True)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None and issubclass(exc_type, KeyboardInterrupt):
            self.shutdown(0)
            self._print_finish_status(cancelled=True)
        else:
            self.finish()


def _validate(config: Config) -> None:
    if not config.enabled:
        return
    if not config.endpoint_id or not config.endpoint_id.strip():
        raise ConfigError("endpoint_id must not be empty")
    url = config.server_url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ConfigError(f"invalid server_url: {config.server_url!r}")
    if config.client_secret and config.bearer_token_provider:
        raise ConfigError(
            "client_secret and bearer_token_provider are mutually exclusive"
        )
    seen = set()
    for c in config.channels:
        key = c.name.lower()
        if key in seen:
            raise ConfigError(f"duplicate channel name: {c.name!r}")
        seen.add(key)
        _normalize_name(key, "channel name")
        if len(key) > 200:
            raise ConfigError("channel names must contain at most 200 characters")
    if (
        not 0
        < config.max_event_bytes
        <= min(MAX_RECORD_BYTES, config.memory_queue_bytes // 2)
    ):
        raise ConfigError(
            "max_event_bytes must be positive, at most 64 MiB, and at most half memory_queue_bytes"
        )
    for name in (
        "persist_interval_seconds",
        "persist_threshold_bytes",
        "upload_batch_bytes",
        "upload_timeout_seconds",
        "blacklist_timeout_seconds",
        "retry_base_delay_seconds",
        "retry_max_delay_seconds",
    ):
        if not math.isfinite(getattr(config, name)) or getattr(config, name) <= 0:
            raise ConfigError(f"{name} must be positive")
    if config.max_retries != 0 or config.max_retry_queue_depth != 0:
        logging.getLogger("feed").debug(
            "[feed] durable storage retains retries; legacy retry count/depth limits do not discard events"
        )

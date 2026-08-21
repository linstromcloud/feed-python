"""Internal batching and transport client used by :func:`feed.init`."""

from __future__ import annotations

import uuid
import threading
from dataclasses import dataclass
from typing import Dict, List, Optional

from .channel import ChannelHandle
from .config import DEFAULT_CHANNEL, ChannelSettings, Config
from .delivery import DeliveryReport, DeliveryTracker
from .errors import ConfigError
from .fields import Field, FieldType, _infer_field, _normalize_name
from .state import StateStore
from .worker import Worker, WorkerState


@dataclass(frozen=True)
class Channel:
    """An opaque handle to a channel, returned by :meth:`Client.channel`."""

    index: int


class Client:
    """Internal client owning queues, delivery tracking, and the HTTP worker.

    Applications should use :func:`feed.init` and :class:`feed.Run`. Keeping the
    transport behind this boundary lets the public run API stay independent of
    batching and wire-protocol details.
    """

    def __init__(self, config: Config) -> None:
        _validate(config)
        self._enabled = config.enabled
        self._session_id = str(uuid.uuid4())
        self._state = StateStore()
        self._delivery = DeliveryTracker()
        self._flush_lock = threading.Lock()

        # Channel names are lowercased so lookup/registration is case-insensitive.
        settings: List[ChannelSettings] = []
        for s in config.channels:
            settings.append(ChannelSettings(**{**s.__dict__, "name": s.name.lower()}))
        if not any(s.name == DEFAULT_CHANNEL for s in settings):
            settings.insert(0, ChannelSettings(DEFAULT_CHANNEL))

        self._handles: List[ChannelHandle] = [
            ChannelHandle(s, self._delivery) for s in settings
        ]
        self._channel_index: Dict[str, int] = {
            s.name: i for i, s in enumerate(settings)
        }
        self._default_channel = self._channel_index[DEFAULT_CHANNEL]

        self._worker: Optional[Worker] = None
        if self._enabled:
            self._worker = Worker(
                config, self._session_id, self._handles, self._delivery
            )
            self._worker.start()

    # --- introspection ----------------------------------------------------

    @property
    def session_id(self) -> str:
        """The session UUID generated at construction."""
        return self._session_id

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

    # --- state ------------------------------------------------------------

    def set_state(self, name: str, value) -> None:
        """Set a persistent state field, inferring its type (see
        :meth:`EventBuilder.add`)."""
        f = _infer_field(name, value)
        self._state.set(f.name, f.ftype, f.value)

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

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.shutdown()


def _validate(config: Config) -> None:
    if not config.endpoint_id or not config.endpoint_id.strip():
        raise ConfigError("endpoint_id must not be empty")
    if not config.enabled:
        return
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

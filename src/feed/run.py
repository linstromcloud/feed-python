"""Low-friction run interface built on the generic Feed client."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Union, overload

from .client import Client
from .config import ChannelSettings, Config
from .credentials import authenticated_feed
from .delivery import DeliveryReport
from .fields import EventBuilder

_STANDARD_CHANNELS = (
    ChannelSettings(
        "metadata", priority=-2, flush_threshold_events=1, flush_interval_seconds=0.1
    ),
    ChannelSettings(
        "data",
        priority=0,
        flush_threshold_events=128,
        flush_interval_seconds=1.0,
    ),
)


def init(
    feed: Optional[str] = None,
    *,
    server_url: Optional[str] = None,
    api_key: Optional[str] = None,
    name: Optional[str] = None,
    config: Optional[Mapping[str, Any]] = None,
    tags: Optional[Iterable[str]] = None,
    group: Optional[str] = None,
    enabled: bool = True,
    max_retries: Optional[int] = None,
    max_retry_queue_depth: Optional[int] = None,
) -> "Run":
    """Start a run in a shared feed.

    ``feed`` is the ``project/feed`` reference printed by ``feed list``. It
    defaults to FEED, the feed selected by ``feed use``, or the sole feed
    available at login. Credentials default to FEED_API_KEY and the service
    URL to FEED_URL.
    """
    secret = api_key if api_key is not None else os.environ.get("FEED_API_KEY")
    token_provider = None
    requested_feed = str(feed or os.environ.get("FEED", "")).strip()
    resolved_feed = requested_feed
    feed_reference = requested_feed
    url = server_url or os.environ.get("FEED_URL") or ""
    if secret is None and enabled:
        url, resolved_feed, token_provider, feed_reference = authenticated_feed(
            requested_feed or None, url
        )
    elif enabled:
        if not requested_feed:
            raise ValueError(
                "feed is required with API-key authentication (pass feed or set FEED)"
            )
        if not url:
            raise ValueError("server_url is required (or set FEED_URL)")
        resolved_feed = requested_feed.rsplit("/", 1)[-1]
    client_config = Config(
        server_url=url,
        endpoint_id=resolved_feed,
        client_secret=secret,
        bearer_token_provider=token_provider,
        channels=list(_STANDARD_CHANNELS),
        enabled=enabled,
    )
    if max_retries is not None:
        client_config.max_retries = max_retries
    if max_retry_queue_depth is not None:
        client_config.max_retry_queue_depth = max_retry_queue_depth
    client = Client(client_config)
    run = Run(_client=client, feed=feed_reference)
    if enabled:
        run._emit_run_metadata(
            name=name, config=config or {}, tags=list(tags or ()), group=group
        )
    return run


@dataclass
class Run:
    """One recorded session. Record fields have no implicit semantics."""

    _client: Client = field(repr=False)
    feed: str

    @property
    def id(self) -> str:
        return self._client.session_id

    def _emit_run_metadata(
        self,
        *,
        name: Optional[str],
        config: Mapping[str, Any],
        tags: list[str],
        group: Optional[str],
    ) -> bool:
        if not self._client.enabled:
            return False
        builder = (
            EventBuilder()
            .add_optional_string("name", name)
            .add_string_array("tags", tags)
            .add_optional_string("group", group)
            .add_variant("config", dict(config))
        )
        fields = builder.build()
        return self._client.emit_on(self._client.channel("metadata"), "run", fields)

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
        Returns ``False`` without inspecting the record when this run is
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
        if not self._client.enabled:
            return False
        if not isinstance(data, Mapping):
            raise TypeError("record must be a mapping")
        builder = EventBuilder()
        for field_name, value in data.items():
            builder.add(field_name, value)
        target = self._client.channel("data")
        fields = builder.build()
        if enqueue_timeout is None:
            return self._client.emit_on(target, schema_name, fields)
        return self._client.emit_on_wait(target, schema_name, fields, enqueue_timeout)

    def flush(self, timeout: float = 10.0) -> DeliveryReport:
        """Flush events accepted so far without ending the run."""
        return self._client.flush(timeout)

    def finish(self, timeout: float = 10.0) -> DeliveryReport:
        """Flush pending events, stop the worker, and return its delivery report."""
        return self._client.shutdown(timeout)

    def __enter__(self) -> "Run":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.finish()

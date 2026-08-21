"""Client and channel configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional

#: Default channel name used when ``emit`` is called without an explicit channel.
DEFAULT_CHANNEL = "default"


class Defaults:
    """Built-in transport defaults."""

    QUEUE_CAPACITY = 1024
    FLUSH_THRESHOLD_EVENTS = 500
    FLUSH_INTERVAL_SECONDS = 5.0
    MAX_CONCURRENT_SLOTS = 4
    MAX_CONCURRENT_REQUESTS = 4
    MAX_EVENTS_PER_SECOND = 0  # 0 = unlimited

    RETRY_BASE_DELAY_SECONDS = 1.0
    RETRY_MAX_DELAY_SECONDS = 60.0
    MAX_RETRIES = 5
    MAX_RETRY_QUEUE_DEPTH = 50

    BLACKLIST_TIMEOUT_SECONDS = 10.0
    UPLOAD_TIMEOUT_SECONDS = 30.0
    MAX_BLACKLIST_FETCH_ATTEMPTS = 0  # 0 = retry indefinitely


@dataclass
class ChannelSettings:
    """Per-channel configuration.

    Each channel has its own queue, sequence-number stream, flush triggers, and
    upload priority.
    """

    name: str
    #: Lower numeric values upload first when concurrency is contended.
    priority: int = 0
    #: Capacity of the bounded queue between emitter and worker.
    queue_capacity: int = Defaults.QUEUE_CAPACITY
    #: Flush once this many events accumulate.
    flush_threshold_events: int = Defaults.FLUSH_THRESHOLD_EVENTS
    #: Flush at least this often (seconds).
    flush_interval_seconds: float = Defaults.FLUSH_INTERVAL_SECONDS
    #: 0 disables rate limiting; otherwise a token bucket caps emit throughput.
    max_events_per_second: int = Defaults.MAX_EVENTS_PER_SECOND
    #: Per-channel cap on concurrently in-flight uploads.
    max_concurrent_slots: int = Defaults.MAX_CONCURRENT_SLOTS


@dataclass
class Config:
    """Internal transport configuration assembled by :func:`feed.init`."""

    #: Feed ingest root URL, e.g. ``https://feed.example.com/ingest``.
    server_url: str
    #: Endpoint identifier used as the project-scoped request path segment.
    endpoint_id: str
    #: Optional shared secret sent as the ``X-Client-Secret`` header.
    client_secret: Optional[str] = None
    #: Optional callable returning a current user bearer token. Used by the
    #: project-scoped login flow; mutually exclusive with ``client_secret``.
    bearer_token_provider: Optional[Callable[[], str]] = None
    #: Channel definitions. The default channel is added automatically if absent.
    channels: List[ChannelSettings] = field(default_factory=list)
    #: Client-wide cap on concurrently in-flight uploads.
    max_concurrent_requests: int = Defaults.MAX_CONCURRENT_REQUESTS
    #: Cap on blacklist-fetch retries before telemetry is disabled. 0 = forever.
    max_blacklist_fetch_attempts: int = Defaults.MAX_BLACKLIST_FETCH_ATTEMPTS
    #: Initial retry backoff (seconds).
    retry_base_delay_seconds: float = Defaults.RETRY_BASE_DELAY_SECONDS
    #: Maximum retry backoff (seconds).
    retry_max_delay_seconds: float = Defaults.RETRY_MAX_DELAY_SECONDS
    #: Maximum retries for a single batch before it is dropped. 0 retries
    #: indefinitely, which is suitable for acknowledged export pipelines.
    max_retries: int = Defaults.MAX_RETRIES
    #: Maximum number of batches held awaiting retry. 0 is unlimited.
    max_retry_queue_depth: int = Defaults.MAX_RETRY_QUEUE_DEPTH
    #: Blacklist-fetch request timeout (seconds).
    blacklist_timeout_seconds: float = Defaults.BLACKLIST_TIMEOUT_SECONDS
    #: Telemetry upload request timeout (seconds).
    upload_timeout_seconds: float = Defaults.UPLOAD_TIMEOUT_SECONDS
    #: Master switch. When False, no worker or network connection is created.
    enabled: bool = True

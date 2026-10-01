"""Initialize a Feed client using a saved login or an API key."""

from __future__ import annotations

import os
from typing import Iterable, Optional
from urllib.parse import urlsplit, urlunsplit

from .client import Client
from .config import ChannelSettings, Config
from .credentials import authenticated_feed

Run = Client


def init(
    feed: Optional[str] = None,
    *,
    ingest_url: Optional[str] = None,
    server_url: Optional[str] = None,
    api_key: Optional[str] = None,
    channels: Optional[Iterable[ChannelSettings]] = None,
    enabled: bool = True,
    max_retries: Optional[int] = None,
    max_retry_queue_depth: Optional[int] = None,
    spool_dir: Optional[str] = None,
    max_spool_bytes: Optional[int] = None,
    memory_queue_bytes: int = 16 * 1024**2,
    max_event_bytes: int = 4 * 1024**2,
    persist_interval_seconds: float = 1.0,
    persist_threshold_bytes: int = 256 * 1024,
) -> Client:
    """Start a Feed client with background delivery.

    API-key access uses ``ingest_url`` (``https://host/v1/feed``) and
    ``api_key``, defaulting to FEED_INGEST_URL and FEED_API_KEY. A URL ending
    in ``/telemetry`` is also accepted. ``ingest_url`` cannot be combined
    with explicit ``feed`` or ``server_url`` arguments.

    Saved login uses ``feed``, the ``project/feed`` reference printed by
    ``feed list``. It defaults to FEED, the selected feed, or the sole feed
    available at login. ``server_url`` defaults to FEED_URL.
    """
    secret = api_key if api_key is not None else os.environ.get("FEED_API_KEY")
    token_provider = None
    requested_feed = str(feed or os.environ.get("FEED", "")).strip()
    resolved_feed = requested_feed
    feed_reference = requested_feed
    url = server_url or os.environ.get("FEED_URL") or ""
    ingest_url = (
        ingest_url if ingest_url is not None else os.environ.get("FEED_INGEST_URL")
    )
    if ingest_url is not None and enabled:
        if feed is not None or server_url is not None:
            raise ValueError("ingest_url cannot be combined with feed or server_url")
        if not secret:
            raise ValueError(
                "api_key is required with ingest_url (or set FEED_API_KEY)"
            )
        url, resolved_feed = _ingest_destination(ingest_url)
        feed_reference = resolved_feed
    elif secret is None and enabled:
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
        channels=list(channels) if channels is not None else [],
        enabled=enabled,
        spool_dir=spool_dir,
        max_spool_bytes=max_spool_bytes,
        memory_queue_bytes=memory_queue_bytes,
        max_event_bytes=max_event_bytes,
        persist_interval_seconds=persist_interval_seconds,
        persist_threshold_bytes=persist_threshold_bytes,
        project_id=getattr(token_provider, "project_id", None),
        feed_id=getattr(token_provider, "feed_id", None),
        control_url=getattr(token_provider, "control_url", None),
        feed_reference=feed_reference,
    )
    if max_retries is not None:
        client_config.max_retries = max_retries
    if max_retry_queue_depth is not None:
        client_config.max_retry_queue_depth = max_retry_queue_depth
    return Client(client_config)


def _ingest_destination(ingest_url: str):
    error = (
        "ingest_url must be an HTTP(S) URL ending in /v1/<feed> or "
        "/v1/<feed>/telemetry, without credentials, query or fragment"
    )
    try:
        parsed = urlsplit(ingest_url)
        parsed.port  # Validate the optional port before starting the worker.
    except ValueError:
        raise ValueError(error) from None
    prefix, separator, endpoint = parsed.path.rstrip("/").rpartition("/v1/")
    endpoint = endpoint.removesuffix("/telemetry")
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.query
        or parsed.fragment
        or not separator
        or not endpoint
        or "/" in endpoint
        or endpoint in (".", "..")
    ):
        raise ValueError(error)
    return urlunsplit((parsed.scheme, parsed.netloc, prefix, "", "")), endpoint

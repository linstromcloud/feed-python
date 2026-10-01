"""Project-scoped logging for Python.

Quick start::

    import feed

    with feed.init() as client:
        client.log("readings", {"temperature": 21.4})
"""

from __future__ import annotations

from .delivery import DeliveryReport
from .errors import AuthError, ConfigError
from .client import Channel, Client
from .config import ChannelSettings, Config
from .fields import EventBuilder, Field, FieldType
from .worker import WorkerState
from .run import Run, init

__version__ = "0.1.0"

__all__ = [
    "Client",
    "Config",
    "Channel",
    "ChannelSettings",
    "EventBuilder",
    "Field",
    "FieldType",
    "WorkerState",
    "ConfigError",
    "DeliveryReport",
    "AuthError",
    "init",
    "Run",
    "__version__",
]

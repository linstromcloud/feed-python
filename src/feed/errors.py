"""Exceptions for the Feed client.

Authentication and configuration problems raise. Logging itself is best-effort;
an event that cannot be queued returns ``False`` from ``log``.
"""

from __future__ import annotations


class ConfigError(ValueError):
    """Raised by the client constructor for invalid configuration."""


class AuthError(RuntimeError):
    """Raised when stored user authentication cannot be established or refreshed."""

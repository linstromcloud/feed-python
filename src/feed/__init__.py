"""Project-scoped logging for Python.

Quick start::

    import feed

    with feed.init(name="baseline") as run:
        run.log("train", {"step": 0, "loss": 1.0})
"""

from __future__ import annotations

from .delivery import DeliveryReport
from .errors import AuthError
from .run import Run, init

__version__ = "0.1.0"

__all__ = [
    "DeliveryReport",
    "AuthError",
    "init",
    "Run",
    "__version__",
]

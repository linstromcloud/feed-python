"""Explicit recovery across saved destinations without starting new runs."""

import hashlib
import os
from pathlib import Path

from .config import Config
from .credentials import CredentialStore, authenticated_feed, credential_control_url
from .errors import AuthError
from .spool import SpoolRoot, _read_json, _atomic, default_spool_path
from .uploader import UploadRun


def _resolve(destination, timeout):
    if destination.get("auth") == "api_key":
        key = os.environ.get("FEED_API_KEY")
        key_id = hashlib.sha256(key.encode()).hexdigest() if key is not None else None
        if key_id != destination.get("key_id"):
            raise AuthError(
                "set FEED_API_KEY to the key used for this saved destination"
            )
        if key is None and destination.get("key_id") is not None:
            raise AuthError("FEED_API_KEY is required for this saved destination")
        config = Config(
            destination["server_url"], destination["endpoint_id"], client_secret=key
        )
    else:
        store = CredentialStore()
        credentials = store.load()
        if credential_control_url(credentials) != destination.get("control_url"):
            raise AuthError(
                "log in to the saved destination's deployment before syncing this run"
            )
        if not destination.get("feed_id") or not destination.get("project_id"):
            raise AuthError("saved run has no verified project/feed identity")
        url, endpoint, provider, _ = authenticated_feed(
            destination["feed_id"], store=store
        )
        if (
            provider.project_id != destination["project_id"]
            or provider.feed_id != destination["feed_id"]
        ):
            raise AuthError("resolved project/feed identity differs from the saved run")
        config = Config(url, endpoint, bearer_token_provider=provider)
    config.upload_timeout_seconds = timeout
    config.blacklist_timeout_seconds = timeout
    return config


def _counts(path):
    result = {"pending": 0, "failed": 0}
    for _, _, files in os.walk(path, followlinks=False):
        for name in files:
            if name.endswith(".event"):
                result["pending"] += 1
            elif name.endswith(".failed"):
                result["failed"] += 1
    return result


def status(path=None):
    path = Path(path) if path is not None else default_spool_path()
    result = {"runs": [], "pending": 0, "failed": 0}
    if not path.exists():
        return result
    root = SpoolRoot(path)
    for run_path in root.runs():
        counts = _counts(run_path)
        try:
            metadata = _read_json(run_path / "run.json")
            destination = metadata["destination"]
            item = {
                "session_id": metadata["session_id"],
                "path": str(run_path),
                "feed": destination.get("feed_reference")
                or destination.get("feed_id")
                or destination.get("endpoint_id"),
                **counts,
            }
        except (OSError, ValueError, KeyError) as exc:
            item = {"path": str(run_path), "error": str(exc), **counts}
        result["runs"].append(item)
        for key in counts:
            result[key] += counts[key]
    result["reserved_bytes"] = root.usage()
    result["max_spool_bytes"] = root.max_bytes
    return result


def sync_spools(path=None, timeout=30):
    """Visit every run; attempt each record once, splitting HTTP 413 batches.

    A blocked destination does not stop subsequent runs. Failed records get
    one explicit retry; an incomplete pass exits with retained data and errors.
    """
    result = {
        "delivered": 0,
        "filtered": 0,
        "pending": 0,
        "failed": 0,
        "active": 0,
        "errors": [],
    }
    path = Path(path) if path is not None else default_spool_path()
    if not path.exists():
        return result
    root = SpoolRoot(path)
    for run_path in root.runs():
        spool = context = None
        try:
            spool = root.claim(run_path)
            if spool is None:
                result["active"] += 1
                with root.locked():
                    if run_path.exists():
                        _atomic(run_path / "flush.request", b"sync\n")
                continue
            if not any(spool.counts().values()):
                continue
            config = _resolve(spool.destination, timeout)
            context = UploadRun(spool, config, once=True)
            policy = context.transport.blacklist()
            if policy.kind != "success":
                raise RuntimeError(f"cannot fetch filtering rules: {policy.reason}")
            context.blacklist.set_rules(policy.rules)
            spool.requeue_failed()
            for channel in sorted(context.channels, key=lambda c: c.priority):
                while True:
                    previous = context.cursor[channel.name]
                    batch = context.next_batch(channel)
                    context.apply_transitions()
                    if batch is None:
                        if context.cursor[channel.name] != previous:
                            continue
                        break
                    outcome = context.transport.upload(spool.session_id, batch.events)
                    context.result(batch, outcome)
                    context.apply_transitions()
                    if context.last_error:
                        reason = f"{spool.session_id}: {context.last_error}"
                        if reason not in result["errors"]:
                            result["errors"].append(reason)
        except (OSError, ValueError, TypeError, KeyError, RuntimeError) as exc:
            result["errors"].append(f"{run_path.name}: {exc}")
        finally:
            if context is not None:
                result["delivered"] += context.totals["delivered"]
                result["filtered"] += context.totals["filtered"]
                context.transport.close()
            counts = _counts(run_path)
            result["pending"] += counts["pending"]
            result["failed"] += counts["failed"]
            if spool is not None:
                try:
                    spool.close()
                except OSError as exc:
                    result["errors"].append(
                        f"{run_path.name}: cannot clean spool: {exc}"
                    )
    return result

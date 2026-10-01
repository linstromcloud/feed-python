"""Bounded, exclusively owned run spools on a shared filesystem.

Atomic rename publishes synced event batches and delivery checkpoints.
Retry metadata never changes the event identity.
A root lock coordinates disk reservations; producers consume reserved credit
in memory. Filesystems must support process locks, atomic rename and file fsync.
"""

from __future__ import annotations

import heapq
import json
import os
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

from .errors import ConfigError
from ._filesystem import lock, sync_directory as _sync_directory

ROOT_OVERHEAD = 32768
RUN_OVERHEAD = 32768
BLOCK = 4096
MAX_RECORD_BYTES = 64 * 1024 * 1024


def record_cost(size):
    # Event blocks plus retry/error metadata and its atomic replacement.
    return ((size + BLOCK - 1) // BLOCK) * BLOCK + 3 * BLOCK


def default_spool_path():
    return Path(
        os.environ.get(
            "FEED_SPOOL_DIR",
            str(
                Path(
                    os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))
                )
                / "feed/spool"
            ),
        )
    ).expanduser()


def _atomic(path, data):
    temporary = path.parent / (".tmp-" + uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _json(path, value):
    payload = json.dumps(value, separators=(",", ":")).encode()
    if len(payload) > 8192:
        raise ValueError("spool metadata exceeds its reserved 8192-byte budget")
    _atomic(path, payload)


def _read_json(path, limit=65536):
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError(f"spool file exceeds its size limit: {path.name}")
    return json.loads(data)


def _lock_owner(path):
    fd = os.open(path / "owner.lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        lock(fd, blocking=False)
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise
    return fd


def same_destination(left, right):
    keys = ("control_url", "project_id", "feed_id")
    if all(left.get(key) and right.get(key) for key in keys):
        return all(left[key] == right[key] for key in keys)
    # API-key destinations have no project catalog. Match the complete origin
    # and endpoint only when both identities are explicitly API-key based.
    return (
        left.get("auth") == right.get("auth") == "api_key"
        and left.get("key_id") == right.get("key_id")
        and left.get("feed_reference") == right.get("feed_reference")
        and left.get("server_url") == right.get("server_url")
        and left.get("endpoint_id") == right.get("endpoint_id")
    )


class SpoolRoot:
    def __init__(self, path=None, max_bytes=None):
        self.path = Path(path) if path is not None else default_spool_path()
        self.path.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.max_bytes = max_bytes
        if max_bytes is not None and (
            not isinstance(max_bytes, int) or max_bytes < ROOT_OVERHEAD + RUN_OVERHEAD
        ):
            raise ConfigError("max_spool_bytes must be an integer of at least 65536")
        with self.locked():
            policy = self.path / "budget.json"
            if policy.exists():
                stored = _read_json(policy)
                if stored.get("version") not in (1, 2):
                    raise ConfigError("unsupported Feed spool format")
                if stored["version"] == 1 and not self._legacy_owner_active():
                    stored["version"] = 2
                    _json(policy, stored)
                self.version = stored["version"]
                if max_bytes is not None and max_bytes != stored["max_bytes"]:
                    if max_bytes < self._usage():
                        raise ConfigError(
                            "max_spool_bytes is below current stored and reserved usage"
                        )
                    _json(policy, {"version": self.version, "max_bytes": max_bytes})
                    self.max_bytes = max_bytes
                else:
                    self.max_bytes = stored["max_bytes"]
            else:
                self.max_bytes = max_bytes if max_bytes is not None else 1024**3
                if self.max_bytes < ROOT_OVERHEAD + RUN_OVERHEAD:
                    raise ConfigError("max_spool_bytes must be at least 65536")
                self.version = 2
                _json(policy, {"version": self.version, "max_bytes": self.max_bytes})

    @contextmanager
    def locked(self):
        fd = os.open(self.path / "quota.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            lock(fd)
            policy = self.path / "budget.json"
            if policy.exists():
                stored = _read_json(policy)
                if stored.get("version") not in (1, 2):
                    raise ConfigError("unsupported Feed spool format")
                self.max_bytes = stored["max_bytes"]
                self.version = stored["version"]
            yield
        finally:
            os.close(fd)

    def _legacy_owner_active(self):
        for path in self.runs():
            try:
                if _read_json(path / "run.json").get("version") != 1:
                    continue
            except FileNotFoundError:
                continue
            fd = _lock_owner(path)
            if fd is None:
                return True
            os.close(fd)
        return False

    def runs(self, destination=None):
        with os.scandir(self.path) as entries:
            for entry in entries:
                if not entry.is_dir(follow_symlinks=False):
                    continue
                path = Path(entry.path)
                if destination is None:
                    yield path
                else:
                    try:
                        if same_destination(
                            _read_json(path / "run.json")["destination"], destination
                        ):
                            yield path
                    except (OSError, ValueError, KeyError):
                        continue

    @staticmethod
    def _stored_usage(path):
        try:
            version = _read_json(path / "run.json").get("version")
        except FileNotFoundError:
            version = 1
        if version == 2:
            from .spool_batch import stored_usage

            return stored_usage(path)
        usage = RUN_OVERHEAD
        for directory, _, names in os.walk(path, followlinks=False):
            for name in names:
                entry = Path(directory) / name
                orphan_retry = name.endswith(".retry") and not any(
                    entry.with_suffix(suffix).exists()
                    for suffix in (".event", ".failed")
                )
                if (
                    orphan_retry
                    or name.endswith((".event", ".failed"))
                    or name.startswith(".tmp-")
                ):
                    usage += record_cost(entry.stat().st_size)
        return usage

    def _usage(self, owned=None):
        usage = ROOT_OVERHEAD
        for path in self.runs():
            fd = None if path == owned else _lock_owner(path)
            if fd is None:
                # Live owners reserve event, temporary and metadata space
                # before writing. Scanning their changing files would both
                # double-count staging and race acknowledgements.
                stored = _read_json(path / "quota.json")["bytes"]
            else:
                try:
                    stored = self._stored_usage(path)
                finally:
                    os.close(fd)
            usage += stored
        return usage

    def usage(self):
        with self.locked():
            return self._usage()

    def create(self, destination, session_id, reserve_bytes):
        if not session_id or Path(session_id).name != session_id:
            raise ValueError("invalid session id")
        with self.locked():
            available = self.max_bytes - self._usage() - RUN_OVERHEAD
            if available < 0:
                raise OSError(
                    "Feed spool budget has no room for another run; run feed sync"
                )
            path = self.path / session_id
            path.mkdir(mode=0o700)
            fd = _lock_owner(path)
            try:
                allocation = RUN_OVERHEAD + min(reserve_bytes, available)
                _json(
                    path / "run.json",
                    {
                        "version": self.version,
                        "session_id": session_id,
                        "destination": destination,
                    },
                )
                _json(path / "quota.json", {"bytes": allocation})
                _sync_directory(self.path)
                return _spool_type(self.version)(
                    self,
                    path,
                    fd,
                    destination,
                    session_id,
                    allocation,
                    allocation - RUN_OVERHEAD,
                )
            except BaseException:
                os.close(fd)
                raise

    def claim(self, path):
        path = Path(path)
        with self.locked():
            if not path.is_dir():
                return None
            fd = _lock_owner(path)
            if fd is None:
                return None
            try:
                metadata = _read_json(path / "run.json")
                if (
                    metadata.get("version") not in (1, 2)
                    or metadata["session_id"] != path.name
                ):
                    raise ValueError("invalid Feed run metadata")
                # Unpublished temporary files belong to the documented
                # unpersisted crash window. An owner must be absent first.
                for entry in path.rglob(".tmp-*"):
                    entry.unlink()
                allocation = self._stored_usage(path)
                _json(path / "quota.json", {"bytes": allocation})
                return _spool_type(metadata["version"])(
                    self,
                    path,
                    fd,
                    metadata["destination"],
                    metadata["session_id"],
                    allocation,
                    0,
                )
            except BaseException:
                os.close(fd)
                raise


def _spool_type(version):
    if version == 1:
        return RunSpool
    from .spool_batch import BatchSpool

    return BatchSpool


class RunSpool:
    batch_limit = 1
    batch_bytes = 256 * 1024
    cost = staticmethod(record_cost)

    def __init__(
        self, root, path, owner_fd, destination, session_id, allocation, credit
    ):
        self.root, self.path, self._owner_fd = root, path, owner_fd
        self.destination, self.session_id = destination, session_id
        self._allocation, self._credit = allocation, credit
        self._mutex = threading.Lock()
        self._closed = False
        # Ownership excludes other writers. Index filenames once, then update
        # this bounded metadata index as this owner publishes/removes records.
        self._entries = {}
        for directory in self.path.iterdir():
            if directory.is_dir():
                for entry in directory.iterdir():
                    if entry.suffix in (".event", ".failed"):
                        self._entries[int(entry.stem)] = (directory.name, entry.suffix)
        self._manifest = _read_json(self.path / "run.json")

    def reserve(self, size):
        with self._mutex:
            cost = self.cost(size)
            if self._closed or cost > self._credit:
                return False
            self._credit -= cost
            return True

    def refund(self, size):
        with self._mutex:
            self._credit += self.cost(size)

    def replenish(self, target):
        with self.root.locked(), self._mutex:
            available = self.root.max_bytes - self.root._usage(self.path)
            desired = target - self._credit
            extra = desired if desired < 0 else max(0, min(desired, available))
            if extra:
                allocation = self._allocation + extra
                _json(self.path / "quota.json", {"bytes": allocation})
                self._allocation = allocation
                self._credit += extra

    def release_unused(self):
        if self._credit:
            self.replenish(0)

    def _file(self, ticket, suffix="event", channel=None):
        if channel is None:
            channel = self._entries.get(ticket, ("default", ".event"))[0]
        return self.path / (channel or "default") / f"{ticket:020d}.{suffix}"

    def persist(self, ticket, payload, channel=None):
        if channel is None:
            channel = json.loads(payload).get("channel", "default")
        if (
            not isinstance(channel, str)
            or not channel.isascii()
            or not channel.replace("_", "").isalnum()
        ):
            raise ValueError("invalid spool channel")
        directory = self.path / channel
        if not directory.exists():
            directory.mkdir(mode=0o700)
            _sync_directory(self.path)
        target = self._file(ticket, channel=channel)
        if target.exists():
            if target.read_bytes() != payload:
                raise ValueError("spool ticket collision")
            _sync_directory(target.parent)
            _sync_directory(self.path)
        else:
            _atomic(target, payload)
        self._entries[ticket] = (channel, ".event")

    def persist_batch(self, records):
        ticket, payload, channel = records[0]
        self.persist(ticket, payload, channel)
        return 1

    def size(self, ticket):
        return self._file(ticket).stat().st_size

    def read_batch(self, tickets):
        for ticket in tickets:
            with self._file(ticket).open("rb") as handle:
                yield ticket, handle.read(MAX_RECORD_BYTES + 1)

    def prepare_batch(self, events, blacklist):
        return {event["ticket"] for event in events if self.prepare(event, blacklist)}

    def ack_batch(self, records):
        for ticket, channel, size in records:
            self.ack(ticket, channel, size)

    def fail_batch(self, records):
        for ticket, reason in records:
            self.fail(ticket, reason)

    def read(self, ticket):
        return _read_json(self._file(ticket), MAX_RECORD_BYTES)

    def ready(self, limit, exclude=(), channel=None, after=-1):
        # Retry deadlines belong to in-memory batches, never per-event files.
        return heapq.nsmallest(
            limit,
            (
                ticket
                for ticket, (name, suffix) in self._entries.items()
                if suffix == ".event"
                and (channel is None or name == channel)
                and ticket > after
                and ticket not in exclude
            ),
        )

    def _retry_metadata(self, ticket):
        try:
            return _read_json(self._file(ticket, "retry"))
        except FileNotFoundError:
            return {}

    def prepare(self, event, blacklist):
        """Persist one filtering decision and wire sequence before sending.

        Filter checkpoints preserve the existing per-channel gap compaction.
        The event decision precedes the checkpoint so a crash can finish that
        transition without changing an identity that may have reached ingest.
        """
        ticket, channel = event["ticket"], event["channel"]
        retry = self._retry_metadata(ticket)
        if "wire_seq" in retry:
            event["seq"] = retry["wire_seq"]
            return False
        manifest = {
            **self._manifest,
            "filtered": dict(self._manifest.get("filtered", {})),
        }
        checkpoints = manifest.setdefault("filtered", {})
        checkpoint = checkpoints.get(channel, {"ticket": -1, "count": 0})
        if "filtered_count" not in retry:
            if blacklist.is_blacklisted(event["schema_hash"], event["data"]):
                retry["filtered_count"] = checkpoint["count"] + 1
            else:
                retry["wire_seq"] = event["seq"] - checkpoint["count"]
            _json(self._file(ticket, "retry"), retry)
        if "filtered_count" in retry:
            if checkpoint["ticket"] < ticket:
                checkpoints[channel] = {
                    "ticket": ticket,
                    "count": retry["filtered_count"],
                }
                _json(self.path / "run.json", manifest)
                self._manifest = manifest
            return True
        event["seq"] = retry["wire_seq"]
        return False

    def fail(self, ticket, reason):
        target = self._file(ticket)
        if target.with_suffix(".failed").exists():
            _sync_directory(target.parent)
            self._entries[ticket] = (target.parent.name, ".failed")
            return
        metadata = self._retry_metadata(ticket)
        metadata.update(after=0, reason=str(reason)[:1000])
        _json(target.with_suffix(".retry"), metadata)
        os.replace(target, target.with_suffix(".failed"))
        _sync_directory(target.parent)
        self._entries[ticket] = (target.parent.name, ".failed")

    def requeue_failed(self):
        """An explicit sync retries failed records once, preserving identities."""
        for directory in self.path.iterdir():
            if not directory.is_dir():
                continue
            with os.scandir(directory) as entries:
                for entry in entries:
                    if entry.name.endswith(".failed"):
                        target = Path(entry.path)
                        os.replace(target, target.with_suffix(".event"))
                        self._entries[int(target.stem)] = (directory.name, ".event")
            _sync_directory(directory)

    def error(self, ticket):
        return _read_json(self._file(ticket, "retry"))["reason"]

    def ack(self, ticket, channel=None, size=None):
        target = self._file(ticket, channel=channel)
        size = target.stat().st_size if size is None else size
        target.unlink(missing_ok=True)
        target.with_suffix(".retry").unlink(missing_ok=True)
        _sync_directory(target.parent)
        self.refund(size)
        self._entries.pop(ticket, None)

    def counts(self):
        failed = sum(suffix == ".failed" for _, suffix in self._entries.values())
        return {"pending": len(self._entries) - failed, "failed": failed}

    def close(self):
        with self.root.locked(), self._mutex:
            if self._closed:
                return
            self._closed = True
            # The root lock excludes claims while Windows releases the handle
            # required to delete owner.lock and its containing directory.
            os.close(self._owner_fd)
            self._owner_fd = None
            counts = self.counts()
            if not any(counts.values()):
                for path in self.path.iterdir():
                    if path.is_dir():
                        for child in path.iterdir():
                            child.unlink()
                        path.rmdir()
                    else:
                        path.unlink()
                self.path.rmdir()
                _sync_directory(self.root.path)

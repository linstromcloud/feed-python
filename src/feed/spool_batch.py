"""Immutable event batches with atomic, shared delivery checkpoints."""

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
import json
import struct

from . import spool as storage

COUNT = struct.Struct(">I")
RECORD = struct.Struct(">QI")
MAX_EVENTS = 500
STATE_BYTES = 1280


def batch_count(path):
    with path.open("rb") as handle:
        header = handle.read(COUNT.size)
    if len(header) != COUNT.size:
        raise ValueError("incomplete spool batch header")
    count = COUNT.unpack(header)[0]
    if not 1 <= count <= MAX_EVENTS:
        raise ValueError("invalid spool batch count")
    return count


def read_batch_file(path):
    limit = storage.MAX_RECORD_BYTES + COUNT.size + RECORD.size
    with path.open("rb") as handle:
        payload = handle.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("spool batch exceeds its size limit")
    return payload


def batch_cost(size, count):
    block = storage.BLOCK
    return (
        (size + block - 1) // block
        + 2 * ((64 + count * STATE_BYTES + block - 1) // block)
        + 1
    ) * block


def stored_usage(path):
    usage = storage.RUN_OVERHEAD
    for entry in path.rglob("*"):
        if entry.suffix == ".batch":
            usage += batch_cost(entry.stat().st_size, batch_count(entry))
        elif entry.is_file() and (
            entry.name.startswith(".tmp-")
            or (entry.suffix == ".state" and not entry.with_suffix(".batch").exists())
        ):
            usage += storage.record_cost(entry.stat().st_size)
    return usage


def counts(path):
    result = {"pending": 0, "failed": 0}
    for entry in path.rglob("*.batch"):
        try:
            count = batch_count(entry)
            state = read_state(entry, count)
        except FileNotFoundError:
            continue
        failed = sum(row.get("status") == "failed" for row in state.values())
        acked = sum(row.get("status") == "ack" for row in state.values())
        result["pending"] += count - acked - failed
        result["failed"] += failed
    return result


def read_state(path, count):
    try:
        value = storage._read_json(path.with_suffix(".state"), 64 + count * STATE_BYTES)
    except FileNotFoundError:
        return {}
    if (
        not isinstance(value, dict)
        or value.get("version") != 1
        or not isinstance(value.get("records"), dict)
    ):
        raise ValueError("invalid spool batch state")
    records = value["records"]
    if len(records) > count or any(
        not isinstance(row, dict)
        or row.get("status", "pending") not in ("pending", "failed", "ack")
        for row in records.values()
    ):
        raise ValueError("invalid spool record state")
    return records


def error_text(reason):
    text = str(reason)[:1000]
    low, high = 0, len(text)
    while low < high:
        end = (low + high + 1) // 2
        if len(json.dumps(text[:end], ensure_ascii=False).encode()) <= 1024:
            low = end
        else:
            high = end - 1
    return text[:low]


@dataclass
class Segment:
    path: Path
    tickets: list
    size: int
    state: dict

    @property
    def cost(self):
        return batch_cost(self.size, len(self.tickets))


class BatchSpool(storage.RunSpool):
    batch_limit = MAX_EVENTS

    @staticmethod
    def cost(size):
        return storage.record_cost(size + RECORD.size + COUNT.size)

    def __init__(self, *args):
        super().__init__(*args)
        self._segments = {}
        self._locations = {}
        for path in self.path.rglob("*.batch"):
            segment = self._register(path)
            if all(
                segment.state.get(str(t), {}).get("status") == "ack"
                for t in segment.tickets
            ):
                self._delete(segment)
        for path in self.path.rglob("*.state"):
            if not path.with_suffix(".batch").exists():
                state = read_state(path.with_suffix(".batch"), MAX_EVENTS)
                if not state or any(
                    row.get("status") != "ack" for row in state.values()
                ):
                    raise ValueError("spool checkpoint has no event batch")
                cost = storage.record_cost(path.stat().st_size)
                path.unlink()
                storage._sync_directory(path.parent)
                self._credit += cost

    def _register(self, path, payload=None):
        count = batch_count(path)
        state = read_state(path, count)
        payload = read_batch_file(path) if payload is None else payload
        locations = {}
        offset = COUNT.size
        for _ in range(count):
            if offset + RECORD.size > len(payload):
                raise ValueError("incomplete spool record header")
            ticket, size = RECORD.unpack_from(payload, offset)
            offset += RECORD.size
            if (
                size > storage.MAX_RECORD_BYTES
                or ticket in locations
                or ticket in self._locations
            ):
                raise ValueError("invalid spool record header")
            locations[ticket] = (offset, size)
            offset += size
        if offset != len(payload) or set(state) - {str(t) for t in locations}:
            raise ValueError("spool batch identity or length mismatch")
        segment = Segment(path, list(locations), len(payload), state)
        self._segments[path] = segment
        for ticket, (offset, length) in locations.items():
            self._locations[ticket] = (segment, offset, length)
            status = state.get(str(ticket), {}).get("status")
            if status != "ack":
                self._entries[ticket] = (
                    path.parent.name,
                    ".failed" if status == "failed" else ".event",
                )
        return segment

    def persist(self, ticket, payload, channel=None):
        channel = (
            channel
            if channel is not None
            else json.loads(payload).get("channel", "default")
        )
        return self.persist_batch([(ticket, payload, channel)])

    def persist_batch(self, records):
        selected, size = [], COUNT.size
        for record in records[:MAX_EVENTS]:
            length = RECORD.size + len(record[1])
            if selected and size + length > self.batch_bytes:
                break
            selected.append(record)
            size += length
        records = selected
        first, _, channel = records[0]
        if (
            not isinstance(channel, str)
            or not channel.isascii()
            or not channel.replace("_", "").isalnum()
        ):
            raise ValueError("invalid spool channel")
        if any(name != channel for _, _, name in records):
            raise ValueError("spool batch crosses channels")
        directory = self.path / channel
        if not directory.exists():
            directory.mkdir(mode=0o700)
            storage._sync_directory(self.path)
        path = directory / f"{first:020d}.batch"
        if path.exists():
            count = batch_count(path)
            if count > len(records):
                raise ValueError("spool ticket collision")
            records = records[:count]
        payload = COUNT.pack(len(records)) + b"".join(
            RECORD.pack(ticket, len(body)) + body for ticket, body, _ in records
        )
        if path.exists():
            if path.read_bytes() != payload:
                raise ValueError("spool ticket collision")
            storage._sync_directory(directory)
            storage._sync_directory(self.path)
        else:
            storage._atomic(path, payload)
        if path not in self._segments:
            segment = self._register(path, payload)
            with self._mutex:
                self._credit += (
                    sum(self.cost(len(p)) for _, p, _ in records) - segment.cost
                )
        return len(records)

    def size(self, ticket):
        return self._locations[ticket][2]

    def read(self, ticket):
        segment, offset, size = self._locations[ticket]
        with segment.path.open("rb") as handle:
            handle.seek(offset)
            value = handle.read(size)
        if len(value) != size:
            raise ValueError("incomplete spool record")
        return json.loads(value)

    def read_batch(self, tickets):
        path, payload = None, None
        for ticket in tickets:
            segment, offset, size = self._locations[ticket]
            if path != segment.path:
                path = segment.path
                payload = read_batch_file(path)
            yield ticket, payload[offset : offset + size]

    def _save(self, segment, state):
        if state == segment.state:
            return
        data = json.dumps(
            {"version": 1, "records": state}, separators=(",", ":"), ensure_ascii=False
        ).encode()
        if len(data) > 64 + len(segment.tickets) * STATE_BYTES:
            raise ValueError("spool batch state exceeds its reserved budget")
        storage._atomic(segment.path.with_suffix(".state"), data)
        segment.state = state
        for ticket in segment.tickets:
            status = state.get(str(ticket), {}).get("status")
            if status == "ack":
                self._entries.pop(ticket, None)
            else:
                self._entries[ticket] = (
                    segment.path.parent.name,
                    ".failed" if status == "failed" else ".event",
                )

    def prepare_batch(self, events, blacklist):
        changes = {}
        manifest = {
            **self._manifest,
            "filtered": dict(self._manifest.get("filtered", {})),
        }
        filtered = set()
        for event in events:
            ticket, channel = event["ticket"], event["channel"]
            segment = self._locations[ticket][0]
            if segment.path not in changes:
                changes[segment.path] = dict(segment.state)
            state = changes[segment.path]
            retry = dict(state.get(str(ticket), {}))
            checkpoint = manifest["filtered"].get(channel, {"ticket": -1, "count": 0})
            if "wire_seq" not in retry and "filtered_count" not in retry:
                if blacklist.is_blacklisted(event["schema_hash"], event["data"]):
                    retry["filtered_count"] = checkpoint["count"] + 1
                else:
                    retry["wire_seq"] = event["seq"] - checkpoint["count"]
                state[str(ticket)] = retry
            if "filtered_count" in retry:
                if checkpoint["ticket"] < ticket:
                    manifest["filtered"][channel] = {
                        "ticket": ticket,
                        "count": retry["filtered_count"],
                    }
                filtered.add(ticket)
            else:
                event["seq"] = retry["wire_seq"]
        for path, state in changes.items():
            self._save(self._segments[path], state)
        if manifest["filtered"] != self._manifest.get("filtered", {}):
            storage._json(self.path / "run.json", manifest)
            self._manifest = manifest
        return filtered

    def prepare(self, event, blacklist):
        return event["ticket"] in self.prepare_batch([event], blacklist)

    def _update(self, records):
        changes = defaultdict(dict)
        for ticket, update in records:
            if ticket not in self._locations:
                continue
            segment = self._locations[ticket][0]
            changes[segment.path][str(ticket)] = {
                **segment.state.get(str(ticket), {}),
                **update,
            }
        for path, updates in changes.items():
            segment = self._segments[path]
            self._save(segment, {**segment.state, **updates})
            if all(
                segment.state.get(str(t), {}).get("status") == "ack"
                for t in segment.tickets
            ):
                self._delete(segment)

    def _delete(self, segment):
        segment.path.unlink(missing_ok=True)
        segment.path.with_suffix(".state").unlink(missing_ok=True)
        storage._sync_directory(segment.path.parent)
        for ticket in segment.tickets:
            self._locations.pop(ticket, None)
            self._entries.pop(ticket, None)
        self._segments.pop(segment.path)
        with self._mutex:
            self._credit += segment.cost

    def ack_batch(self, records):
        self._update((ticket, {"status": "ack"}) for ticket, _, _ in records)

    def ack(self, ticket, channel=None, size=None):
        self.ack_batch([(ticket, channel, size)])

    def fail_batch(self, records):
        self._update(
            (ticket, {"status": "failed", "reason": error_text(reason)})
            for ticket, reason in records
        )

    def fail(self, ticket, reason):
        self.fail_batch([(ticket, reason)])

    def requeue_failed(self):
        self._update(
            (ticket, {"status": "pending"})
            for ticket, (_, suffix) in list(self._entries.items())
            if suffix == ".failed"
        )

    def error(self, ticket):
        return self._locations[ticket][0].state[str(ticket)]["reason"]

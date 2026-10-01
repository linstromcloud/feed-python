"""Durability, quota and compatibility of batched spool storage."""

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from feed.blacklist import Blacklist, BlacklistRule
from feed.spool import ROOT_OVERHEAD, RUN_OVERHEAD, SpoolRoot, record_cost


DESTINATION = {"server_url": "https://feed.test", "endpoint_id": "test"}


def records(start=0, count=100, channel="default"):
    result = []
    for ticket in range(start, start + count):
        payload = json.dumps(
            {
                "ticket": ticket,
                "channel": channel,
                "seq": ticket,
                "schema_hash": "metric",
                "schema_def": {"value": "int64"},
                "data": {"value": ticket},
            },
            separators=(",", ":"),
        ).encode()
        result.append((ticket, payload, channel))
    return result


def persist(spool, items):
    spool.replenish(sum(spool.cost(len(payload)) for _, payload, _ in items))
    for _, payload, _ in items:
        assert spool.reserve(len(payload))
    remaining = list(items)
    while remaining:
        count = spool.persist_batch(remaining)
        assert count > 0
        del remaining[:count]
    spool.release_unused()


def legacy_run(path, items):
    path.mkdir(parents=True)
    (path / "run.json").write_text(
        json.dumps(
            {
                "version": 1,
                "session_id": path.name,
                "destination": DESTINATION,
            }
        )
    )
    allocation = RUN_OVERHEAD
    for ticket, payload, channel in items:
        directory = path / channel
        directory.mkdir(exist_ok=True)
        (directory / f"{ticket:020d}.event").write_bytes(payload)
        allocation += record_cost(len(payload))
    (path / "quota.json").write_text(json.dumps({"bytes": allocation}))
    (path.parent / "budget.json").write_text(
        json.dumps({"version": 1, "max_bytes": 2**24})
    )


def test_batch_storage_amortizes_files_syncs_and_quota(tmp_path):
    import feed.spool as storage

    root = SpoolRoot(tmp_path, 16 * 1024**2)
    spool = root.create(DESTINATION, "session", 0)
    items = records(count=500)
    try:
        with patch.object(storage.os, "fsync", wraps=os.fsync) as sync:
            persist(spool, items)
            events = [spool.read(ticket) for ticket, _, _ in items]
            assert spool.prepare_batch(events, Blacklist()) == set()
        assert sync.call_count < 20
        assert len(list(spool.path.rglob("*.batch"))) == 1
        assert len(list(spool.path.rglob("*.state"))) == 1
        assert root.usage() < ROOT_OVERHEAD + RUN_OVERHEAD + 2 * 1024**2
        spool.close()
        spool = root.claim(tmp_path / "session")
        assert spool.ready(1000) == list(range(500))
        assert [spool.read(i) for i in range(500)] == [
            json.loads(p) for _, p, _ in items
        ]
        spool.ack_batch([(i, "default", len(p)) for i, p, _ in items])
        spool.release_unused()
        assert root.usage() == ROOT_OVERHEAD + RUN_OVERHEAD
    finally:
        spool.close()
    assert not list(root.runs())


def test_partial_ack_and_failure_recover_without_replaying_acked_rows(tmp_path):
    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    items = records(count=5)
    persist(spool, items)
    spool.prepare_batch([spool.read(i) for i in range(5)], Blacklist())
    spool.ack_batch(
        [(0, "default", len(items[0][1])), (2, "default", len(items[2][1]))]
    )
    spool.fail_batch([(1, "rejected payload")])
    spool.close()
    recovered = root.claim(tmp_path / "session")
    try:
        assert recovered.ready(10) == [3, 4]
        assert recovered.counts() == {"pending": 2, "failed": 1}
        assert recovered.error(1) == "rejected payload"
        recovered.requeue_failed()
        assert recovered.ready(10) == [1, 3, 4]
        events = [recovered.read(i) for i in [1, 3, 4]]
        assert recovered.prepare_batch(events, Blacklist()) == set()
        assert [event["seq"] for event in events] == [1, 3, 4]
        recovered.ack_batch([(i, "default", len(items[i][1])) for i in [1, 3, 4]])
    finally:
        recovered.close()
    assert not list(root.runs())


def test_batch_filtering_checkpoint_survives_failed_manifest_sync(
    tmp_path, monkeypatch
):
    import feed.spool as storage

    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    persist(spool, records(count=4))
    policy = Blacklist()
    policy.set_rules(
        [
            BlacklistRule("metric", {"value": "0"}),
            BlacklistRule("metric", {"value": "2"}),
        ]
    )
    original = storage._atomic

    def fail_manifest(path, data):
        if path.name == "run.json":
            raise OSError("manifest unavailable")
        original(path, data)

    with monkeypatch.context() as patcher:
        patcher.setattr(storage, "_atomic", fail_manifest)
        with pytest.raises(OSError, match="manifest unavailable"):
            spool.prepare_batch([spool.read(i) for i in range(4)], policy)
    spool.close()
    recovered = root.claim(tmp_path / "session")
    try:
        events = [recovered.read(i) for i in range(4)]
        assert recovered.prepare_batch(events, Blacklist()) == {0, 2}
        assert [events[i]["seq"] for i in [1, 3]] == [0, 1]
        persist(recovered, records(start=4, count=1))
        later = recovered.read(4)
        assert recovered.prepare_batch([later], Blacklist()) == set()
        assert later["seq"] == 2
    finally:
        recovered.close()


def test_retry_after_publication_does_not_duplicate_an_expanded_batch(
    tmp_path, monkeypatch
):
    import feed.spool as storage

    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 1024**2)
    items = records(count=4)
    for _, payload, _ in items:
        assert spool.reserve(len(payload))
    original = storage._sync_directory

    def fail_after_rename(path):
        if list(path.glob("*.batch")):
            raise OSError("directory sync unavailable")
        original(path)

    try:
        with monkeypatch.context() as patcher:
            patcher.setattr(storage, "_sync_directory", fail_after_rename)
            with pytest.raises(OSError):
                spool.persist_batch(items[:2])
        assert spool.persist_batch(items) == 2
        assert spool.persist_batch(items[2:]) == 2
        spool.close()
        spool = root.claim(tmp_path / "session")
        assert spool.ready(10) == [0, 1, 2, 3]
        assert spool.counts() == {"pending": 4, "failed": 0}
    finally:
        spool.close()


def test_interrupted_batch_cleanup_keeps_acknowledgements_durable(
    tmp_path, monkeypatch
):
    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    items = records(count=3)
    persist(spool, items)
    original = Path.unlink

    def fail_delete(path, *args, **kwargs):
        if path.suffix == ".batch":
            raise OSError("cannot unlink batch")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "unlink", fail_delete)
        with pytest.raises(OSError, match="cannot unlink"):
            spool.ack_batch([(i, "default", len(p)) for i, p, _ in items])
        os.close(spool._owner_fd)
        spool._closed = True
    recovered = root.claim(tmp_path / "session")
    try:
        assert recovered.ready(10) == []
        assert recovered.counts() == {"pending": 0, "failed": 0}
    finally:
        recovered.close()
    assert not list(root.runs())


def test_existing_spool_recovery_and_live_writer_upgrade(tmp_path):
    from feed.spool import _lock_owner

    legacy = tmp_path / "legacy"
    items = records(count=2)
    legacy_run(legacy, items)
    owner = _lock_owner(legacy)
    root = SpoolRoot(tmp_path)
    current = root.create(DESTINATION, "current", 0)
    try:
        assert json.loads((tmp_path / "budget.json").read_text())["version"] == 1
        assert json.loads((current.path / "run.json").read_text())["version"] == 1
    finally:
        current.close()
        os.close(owner)
    root = SpoolRoot(tmp_path)
    assert json.loads((tmp_path / "budget.json").read_text())["version"] == 2
    recovered = root.claim(legacy)
    try:
        assert recovered.ready(10) == [0, 1]
        assert recovered.read(1) == json.loads(items[1][1])
        recovered.ack_batch([(i, "default", len(p)) for i, p, _ in items])
    finally:
        recovered.close()
    assert not list(root.runs())


def test_batches_bound_count_and_bytes_and_keep_oversized_records_whole(tmp_path):
    root = SpoolRoot(tmp_path, 32 * 1024**2)
    spool = root.create(DESTINATION, "session", 0)
    try:
        persist(spool, records(count=1200))
        assert sorted(len(s.tickets) for s in spool._segments.values()) == [
            200,
            500,
            500,
        ]
        large = [
            (i, json.dumps({"ticket": i, "blob": "x" * n}).encode(), "default")
            for i, n in [(1200, 160000), (1201, 160000), (1202, 600000)]
        ]
        persist(spool, large)
        assert all(
            s.size <= spool.batch_bytes or len(s.tickets) == 1
            for s in spool._segments.values()
        )
        assert [spool.read(i)["blob"] for i, _, _ in large] == [
            "x" * n for n in [160000, 160000, 600000]
        ]
    finally:
        spool.close()


def test_failed_metadata_stays_within_quota_and_status_counts_records(tmp_path):
    from feed.sync import status

    root = SpoolRoot(tmp_path, 16 * 1024**2)
    spool = root.create(DESTINATION, "session", 0)
    persist(spool, records(count=500))
    spool.prepare_batch([spool.read(i) for i in range(500)], Blacklist())
    before = root.usage()
    reason = "\x00🦆" * 1000
    spool.fail_batch([(i, reason) for i in range(500)])
    assert root.usage() == before
    assert sum(p.stat().st_size for p in spool.path.rglob("*") if p.is_file()) < before
    spool.close()
    report = status(tmp_path)
    assert report["pending"] == 0 and report["failed"] == 500
    recovered = root.claim(tmp_path / "session")
    try:
        assert len(json.dumps(recovered.error(0), ensure_ascii=False).encode()) <= 1024
        recovered.requeue_failed()
        assert recovered.counts() == {"pending": 500, "failed": 0}
    finally:
        recovered.close()


def test_orphan_ack_checkpoint_releases_space_on_recovery(tmp_path, monkeypatch):
    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    persist(spool, records(count=3))
    persist(spool, records(start=3, count=1))
    original = Path.unlink

    def fail_state_delete(path, *args, **kwargs):
        if path.suffix == ".state":
            raise OSError("cannot unlink checkpoint")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patcher:
        patcher.setattr(Path, "unlink", fail_state_delete)
        with pytest.raises(OSError):
            spool.ack_batch([(i, "default", 0) for i in range(3)])
        os.close(spool._owner_fd)
        spool._closed = True
    before = root.usage()
    recovered = root.claim(tmp_path / "session")
    try:
        assert recovered.ready(10) == [3]
        recovered.release_unused()
        assert root.usage() < before
        assert not list(recovered.path.rglob("*.state"))
    finally:
        recovered.close()


def test_corrupt_payload_is_retained_while_later_record_delivers(tmp_path):
    from feed.config import Config
    from feed.transport import Outcome
    from feed.uploader import UploadRun

    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    persist(spool, [(0, b"not json", "default"), records(start=1, count=1)[0]])
    upload = UploadRun(spool, Config("https://feed.test", "test"), once=True)
    try:
        batch = upload.next_batch(upload.channels[0])
        assert batch.tickets == [1]
        upload.result(batch, Outcome("success"))
        upload.apply_transitions()
        assert spool.counts() == {"pending": 0, "failed": 1}
        assert "corrupt record" in spool.error(0)
    finally:
        upload.transport.close()
        spool.close()


def test_processes_batch_under_shared_quota_and_recover_after_exit(tmp_path):
    import subprocess
    import sys

    script = """
import json, os, sys
from feed.spool import SpoolRoot
root = SpoolRoot(sys.argv[1], 2 * 1024**2)
spool = root.create({}, str(os.getpid()), 0)
print("ready", flush=True)
sys.stdin.readline()
ticket = 0
while True:
    spool.replenish(32 * spool.cost(64))
    group = []
    for _ in range(32):
        body = json.dumps({"ticket":ticket}).encode()
        if not spool.reserve(len(body)):
            break
        group.append((ticket,body,"default"))
        ticket += 1
    if not group:
        break
    assert spool.persist_batch(group) == len(group)
    spool.release_unused()
    assert root.usage() <= root.max_bytes
print(ticket, flush=True)
os._exit(0)
"""
    children = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(tmp_path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    try:
        for child in children:
            assert child.stdout.readline().strip() == "ready"
        for child in children:
            child.stdin.write("go\n")
            child.stdin.flush()
        total = 0
        for child in children:
            output, error = child.communicate(timeout=15)
            assert child.returncode == 0, error
            total += int(output.strip())
        assert total > 0
        root = SpoolRoot(tmp_path)
        assert root.usage() <= root.max_bytes
        recovered_count = 0
        for path in list(root.runs()):
            spool = root.claim(path)
            recovered_count += spool.counts()["pending"]
            spool.close()
        assert recovered_count == total
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()


def test_worker_persists_queued_rows_in_bounded_batches(tmp_path, monkeypatch):
    from feed import init
    from feed.worker import Worker

    monkeypatch.setattr(Worker, "start", lambda self: None)
    client = init(
        ingest_url="https://feed.test/v1/test", api_key="test", spool_dir=str(tmp_path)
    )
    spool = client._worker._spool
    try:
        for i in range(1000):
            assert client.log("samples", {"index": i})
        assert client._admission.used > 0
        while not client._handles[0].queue.empty():
            client._worker._persist(force=True)
        assert client._admission.used == 0
        assert spool.ready(1001) == list(range(1000))
        assert 2 <= len(spool._segments) <= 4
        assert [spool.read(i)["data"]["index"] for i in range(1000)] == list(
            range(1000)
        )
    finally:
        client._admission.stop()
        spool.close()
        client._worker._transport.close()


def test_invalid_checkpoint_is_reported_without_deleting_payload(tmp_path):
    from feed.sync import status, sync_spools

    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "session", 0)
    persist(spool, records(count=2))
    path = next(spool.path.rglob("*.batch"))
    spool.close()
    path.with_suffix(".state").write_text("[]")
    report = status(tmp_path)
    assert "invalid spool batch state" in report["runs"][0]["error"]
    synced = sync_spools(tmp_path)
    assert any("invalid spool batch state" in error for error in synced["errors"])
    assert path.exists()

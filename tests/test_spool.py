import json

import pytest

from feed.spool import SpoolRoot, record_cost


DESTINATION = {
    "server_url": "https://feed.test",
    "endpoint_id": "training",
    "project_id": "project-1",
    "feed_id": "feed-1",
    "control_url": "https://control.test",
}


def test_persist_reopen_and_acknowledge(tmp_path):
    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 65536)
    payload = b'{"ticket":0,"channel":"data","seq":0,"data":{"step":1}}'
    assert spool.reserve(len(payload))
    spool.persist(0, payload)
    spool.close()

    recovered = root.claim(tmp_path / "session-1")
    assert recovered is not None
    assert recovered.destination == DESTINATION
    assert recovered.read(0) == json.loads(payload)
    recovered.ack(0)
    recovered.close()
    assert list(root.runs()) == []


def test_one_owner_and_project_identity(tmp_path):
    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 65536)
    assert root.claim(spool.path) is None
    assert list(root.runs({**DESTINATION, "project_id": "project-2"})) == []
    assert list(root.runs(DESTINATION)) == [spool.path]
    spool.close()


def test_shared_quota_counts_reservations(tmp_path):
    root = SpoolRoot(tmp_path, 256 * 1024)
    first = root.create(DESTINATION, "session-1", 128 * 1024)
    second = root.create(DESTINATION, "session-2", 128 * 1024)
    admitted = 0
    for spool in (first, second):
        while spool.reserve(1024):
            admitted += record_cost(1024)
    assert 0 < admitted < root.max_bytes
    assert root.usage() <= root.max_bytes
    first.close()
    second.close()


def test_failed_event_does_not_block_later_records(tmp_path):
    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 128 * 1024)
    for ticket in range(2):
        payload = json.dumps({"ticket": ticket}).encode()
        assert spool.reserve(len(payload))
        spool.persist(ticket, payload)
    spool.fail(0, "HTTP 413: event is too large")
    assert spool.ready(10) == [1]
    assert spool.counts() == {"pending": 1, "failed": 1}
    assert "413" in spool.error(0)
    spool.close()


def test_legacy_retry_timing_does_not_prevent_recovery(tmp_path):
    from feed.blacklist import Blacklist
    from feed.spool import _json

    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 128 * 1024)
    event = {
        "ticket": 0,
        "seq": 0,
        "channel": "data",
        "schema_hash": "metric",
        "data": {},
    }
    payload = json.dumps(event).encode()
    assert spool.reserve(len(payload))
    spool.persist(0, payload)
    _json(spool._file(0, "retry"), {"after": 10**12, "attempts": 9, "wire_seq": 0})
    spool.close()
    recovered = root.claim(tmp_path / "session-1")
    assert recovered.ready(10) == [0]
    assert not recovered.prepare(event, Blacklist())
    assert event["seq"] == 0
    recovered.close()


def test_checkpoint_keeps_filter_compaction_and_retry_identity(tmp_path):
    from feed.blacklist import Blacklist, BlacklistRule

    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 128 * 1024)
    policy = Blacklist()
    policy.set_rules([BlacklistRule("noise", {})])
    for ticket, schema in enumerate(("noise", "metric", "noise", "metric")):
        event = {
            "ticket": ticket,
            "seq": ticket,
            "channel": "data",
            "schema_hash": schema,
            "data": {},
        }
        payload = json.dumps(event).encode()
        assert spool.reserve(len(payload))
        spool.persist(ticket, payload)
        if spool.prepare(event, policy):
            spool.ack(ticket)
        else:
            assert event["seq"] == (0 if ticket == 1 else 1)
    spool.close()
    recovered = root.claim(tmp_path / "session-1")
    for ticket, sequence in ((1, 0), (3, 1)):
        event = recovered.read(ticket)
        assert not recovered.prepare(event, policy)
        assert event["seq"] == sequence
    recovered.close()


def test_acknowledged_space_is_released_to_other_runs(tmp_path):
    root = SpoolRoot(tmp_path, 256 * 1024)
    spool = root.create(DESTINATION, "session-1", 128 * 1024)
    before = root.usage()
    spool.replenish(16384)
    assert root.usage() < before
    other = root.create(DESTINATION, "session-2", 128 * 1024)
    assert other.reserve(1024)
    spool.close()
    other.close()


def test_retrying_publication_syncs_the_event_directory(tmp_path, monkeypatch):
    import feed.spool as storage

    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 65536)
    payload = b'{"ticket":0,"channel":"data"}'
    assert spool.reserve(len(payload))
    original = storage._sync_directory
    synced = []

    def fail_after_rename(path):
        if (path / "00000000000000000000.event").exists():
            raise OSError("directory sync unavailable")
        original(path)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(storage, "_sync_directory", fail_after_rename)
            with pytest.raises(OSError):
                spool.persist(0, payload)
        with monkeypatch.context() as patch:
            patch.setattr(storage, "_sync_directory", lambda path: synced.append(path))
            spool.persist(0, payload)
        assert spool.path / "data" in synced
    finally:
        spool.close()


def test_shared_budget_updates_reach_existing_writers(tmp_path):
    from feed.errors import ConfigError

    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 65536)
    try:
        SpoolRoot(tmp_path, 256 * 1024)
        spool.replenish(2**20)
        assert root.max_bytes == 256 * 1024
        assert root.usage() <= root.max_bytes
        assert SpoolRoot(tmp_path).max_bytes == root.max_bytes
        with pytest.raises(ConfigError):
            SpoolRoot(tmp_path, 128 * 1024)
    finally:
        spool.close()


def test_quota_counts_metadata_left_by_interrupted_acknowledgement(tmp_path):
    root = SpoolRoot(tmp_path, 2**20)
    spool = root.create(DESTINATION, "session-1", 65536)
    try:
        empty_usage = root._stored_usage(spool.path)
        spool.persist(0, b'{"ticket":0}')
        from feed.spool import _json

        _json(spool._file(0, "retry"), {"wire_seq": 0})
        spool._file(0).unlink()
        assert root._stored_usage(spool.path) > empty_usage
    finally:
        spool.close()


def test_concurrent_processes_share_quota_and_release_crashed_reservations(tmp_path):
    import subprocess
    import sys

    script = """
import json, os, sys
from feed.spool import SpoolRoot, record_cost
root = SpoolRoot(sys.argv[1], 512 * 1024)
spool = root.create({"server_url":"https://test", "endpoint_id":"test"}, str(os.getpid()), 65536)
print("ready", flush=True)
sys.stdin.readline()
payload = b'{"ticket":0}'
count = 0
while True:
    if not spool.reserve(len(payload)):
        spool.replenish(65536)
        if not spool.reserve(len(payload)):
            break
    spool.persist(count, payload)
    count += 1
    assert root.usage() <= root.max_bytes
print(count, flush=True)
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
        for child in children:
            output, error = child.communicate(timeout=10)
            assert child.returncode == 0, error
            assert int(output.strip()) > 0
        root = SpoolRoot(tmp_path)
        assert root.usage() <= root.max_bytes
        assert (
            sum(path.stat().st_size for path in tmp_path.rglob("*") if path.is_file())
            <= root.max_bytes
        )
        for path in root.runs():
            recovered = root.claim(path)
            assert recovered is not None
            recovered.close()
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()


def test_empty_spool_closes_owner_before_unlink(tmp_path, monkeypatch):
    import os
    from pathlib import Path

    root = SpoolRoot(tmp_path)
    spool = root.create(DESTINATION, "empty-run", 65536)
    owner_fd = spool._owner_fd
    unlink = Path.unlink

    def check_owner_closed(path, *args, **kwargs):
        if path == spool.path / "owner.lock":
            with pytest.raises(OSError):
                os.fstat(owner_fd)
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", check_owner_closed)
    spool.close()
    spool.close()
    assert not spool.path.exists()


def test_live_owner_is_exclusive_across_processes_and_released_on_exit(tmp_path):
    import subprocess
    import sys

    script = """
import os, sys
from feed.spool import SpoolRoot
root = SpoolRoot(sys.argv[1])
spool = root.create({}, "child", 65536)
spool.persist(0, b'{"ticket":0}')
print("ready", flush=True)
sys.stdin.readline()
os._exit(0)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "ready"
        root = SpoolRoot(tmp_path)
        assert root.claim(tmp_path / "child") is None
        _, error = child.communicate("exit\n", timeout=10)
        assert child.returncode == 0, error
        recovered = root.claim(tmp_path / "child")
        assert recovered is not None
        assert recovered.read(0) == {"ticket": 0}
        recovered.ack(0)
        recovered.close()
        assert list(root.runs()) == []
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()

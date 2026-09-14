"""Regression checks for the resource costs of recovery."""

import json
from unittest.mock import patch

from feed.client import Client
from feed.config import Config
from feed.fields import EventBuilder
from feed.spool import ROOT_OVERHEAD, RUN_OVERHEAD, SpoolRoot
from feed.worker import Worker


def test_one_hundred_idle_runs_do_not_reserve_event_capacity():
    clients = []
    try:
        with patch.object(Worker, "start"):
            for _ in range(100):
                clients.append(Client(Config("https://feed.test", "training")))
        root = clients[0]._worker._spool.root
        assert root.usage() == ROOT_OVERHEAD + 100 * RUN_OVERHEAD
    finally:
        for client in clients:
            client._worker._spool.close()


def test_selecting_a_batch_does_not_read_backlog_files(tmp_path):
    import feed.spool as storage
    from feed.blacklist import Blacklist

    root = SpoolRoot(tmp_path)
    spool = root.create({}, "run", 8 * 1024**2)
    try:
        for ticket in range(256):
            event = {
                "ticket": ticket,
                "seq": ticket,
                "channel": "data",
                "schema_hash": "metric",
                "data": {"step": ticket},
            }
            payload = json.dumps(event).encode()
            assert spool.reserve(len(payload))
            spool.persist(ticket, payload)
            spool.prepare(event, Blacklist())
        with patch.object(storage, "_read_json", wraps=storage._read_json) as read:
            assert spool.ready(128, channel="data") == list(range(128))
        assert read.call_count == 0
    finally:
        spool.close()


def test_size_is_checked_once_on_the_completed_event(mock_server):
    import feed.limits as limits
    from feed import init

    url, _ = mock_server
    run = init("Project/training", server_url=url, api_key="secret")
    try:
        assert run.flush(5).successful
        with patch.object(limits, "check_size", wraps=limits.check_size) as check:
            assert run.log("metric", {"values": list(range(1000))})
        assert check.call_count == 1
        event = check.call_args.args[0]
        assert event["data"] == {"values": list(range(1000))}
        assert "schema_def" in event and "schema_hash" in event
        assert run.finish(5).successful
    finally:
        run.finish(1)


def test_numeric_size_check_does_not_json_encode_each_number():
    import feed.limits as limits

    with patch.object(limits.json, "dumps", wraps=json.dumps) as dumps:
        assert limits.check_size(list(range(10000)), 1024**2) > 0
    assert dumps.call_count == 0


def test_upload_worker_reuses_and_closes_its_http_session(mock_server, monkeypatch):
    import requests
    from test_recovery import until

    url, _ = mock_server
    sessions, closed = [], []
    original = requests.Session

    class Session(original):
        def __init__(self):
            super().__init__()
            sessions.append(self)

        def close(self):
            closed.append(self)
            super().close()

    monkeypatch.setattr(requests, "Session", Session)
    client = Client(Config(url, "training", max_concurrent_requests=1))
    try:
        for step in range(3):
            assert client.emit("metric", EventBuilder().add("step", step).build())
            assert client.flush(5).successful
        assert len(sessions) == 1
        until(
            lambda: client._worker._spool.root.usage() == ROOT_OVERHEAD + RUN_OVERHEAD
        )
    finally:
        client.shutdown(1)
    until(lambda: closed == sessions)


def test_retry_batches_do_no_disk_writes_and_leave_other_slots_available(tmp_path):
    import feed.spool as storage
    from feed.config import ChannelSettings
    from feed.transport import Outcome
    from feed.uploader import UploadRun

    settings = ChannelSettings(
        "data", flush_threshold_events=128, max_concurrent_slots=2
    )
    root = SpoolRoot(tmp_path)
    spool = root.create({"channels": [settings.__dict__]}, "run", 8 * 1024**2)
    try:
        for ticket in range(256):
            event = {
                "ticket": ticket,
                "seq": ticket,
                "channel": "data",
                "schema_hash": "metric",
                "schema_def": {"step": "int64"},
                "data": {"step": ticket},
            }
            payload = json.dumps(event).encode()
            assert spool.reserve(len(payload))
            spool.persist(ticket, payload)
        uploader = UploadRun(spool, Config("https://feed.test", "training"))
        first = uploader.next_batch(settings)
        assert len(first.events) == 128
        with patch.object(storage.os, "fsync", wraps=storage.os.fsync) as fsync:
            uploader.result(first, Outcome("retry", "HTTP 503", retry_after=60))
        assert fsync.call_count == 0
        assert first.events == []  # waiting retries retain only bounded ticket lists
        second = uploader.next_batch(settings)
        assert second.tickets == list(range(128, 256))
        uploader.result(second, Outcome("success"))
        uploader.apply_transitions()
        assert spool.counts()["pending"] == 128
        assert uploader.next_batch(settings) is None
    finally:
        spool.close()


def test_full_spool_keeps_memory_bounded_and_reports_unsaved(mock_server):
    from feed import init
    from test_recovery import until

    url, server = mock_server
    server.available = False
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        max_spool_bytes=96 * 1024,
        memory_queue_bytes=4096,
        max_event_bytes=1024,
        persist_interval_seconds=0.01,
    )
    try:
        assert run.log({"step": 0})
        until(lambda: run._client._worker._spool.counts()["pending"] == 2)
        accepted = 0
        while accepted < 100 and run.log({"blob": "x" * 500}):
            accepted += 1
        assert 0 < accepted < 100
        assert run._client._admission.used <= 4096
        report = run.finish(0.1)
        assert report.persisted_pending == 2
        assert report.unsaved == accepted
        assert run._client._worker._spool.root.usage() <= 96 * 1024
    finally:
        run.finish(1)


def test_quota_pressure_preserves_filtering_sequence_order(mock_server):
    from feed import init
    from test_integration import _all_events

    url, server = mock_server
    server.blacklist_rules = [{"schema_hash": "*", "match": {"kind": "filtered"}}]
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        max_spool_bytes=128 * 1024,
        max_event_bytes=128 * 1024,
        memory_queue_bytes=256 * 1024,
        persist_interval_seconds=0.01,
    )
    try:
        assert run.flush(5).successful
        assert run.log({"kind": "kept", "blob": "x" * 90000})
        assert run.log({"kind": "filtered"})
        report = run.flush(0.1)
        assert report.unsaved == 2
        assert report.filtered == 0
        SpoolRoot(run._client._worker._spool.root.path, 512 * 1024)
        report = run.flush(5)
        assert report.successful
        assert report.delivered == report.filtered == 1
        assert [
            e["session_sequence_num"]
            for e in _all_events(server)
            if "kind" in e["data"]
        ] == [0]
    finally:
        run.finish(1)

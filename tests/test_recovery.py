"""Outage, process-exit, byte-pressure and large-record acceptance scenarios."""

import json
import subprocess
import sys
import threading
import time

import pytest

from feed import init
from feed.client import Client
from feed.config import ChannelSettings, Config
from feed.fields import EventBuilder
from feed.sync import status, sync_spools
from test_integration import _all_events


def until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.01)
    assert predicate()


def cli_sync():
    process = subprocess.run(
        [sys.executable, "-m", "feed.cli", "sync", "--json", "--timeout", "1"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    return process.returncode, json.loads(process.stdout)


@pytest.mark.parametrize("offline_start", [True, False])
def test_endpoint_outage_recovers_all_accepted_records(mock_server, offline_start):
    url, server = mock_server
    server.available = not offline_start
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        persist_interval_seconds=0.01,
    )
    try:
        if not offline_start:
            assert run.flush(5).successful
            server.available = False
        for step in range(5):
            assert run.log("metric", {"step": step})
        until(lambda: status()["pending"] >= 5)
        assert not run.flush(0.02).successful
        server.available = True
        assert run.flush(5).successful
        assert sorted(
            e["data"]["step"] for e in _all_events(server) if "step" in e["data"]
        ) == list(range(5))
    finally:
        run.finish(1)


def test_outage_does_not_discard_after_legacy_retry_limits(mock_server):
    url, server = mock_server
    client = Client(
        Config(
            url,
            "training",
            client_secret="secret",
            max_retries=1,
            max_retry_queue_depth=1,
            retry_base_delay_seconds=0.001,
            retry_max_delay_seconds=0.005,
            persist_interval_seconds=0.001,
            channels=[ChannelSettings("default", flush_threshold_events=1)],
        )
    )
    try:
        until(lambda: client.worker_state.value == "running")
        server.available = False
        for value in range(10):
            assert client.emit("metric", EventBuilder().add("value", value).build())
        until(lambda: len(server.attempts) >= 15)
        report = client.flush(0)
        assert report.pending == report.persisted_pending == 10
        assert report.dropped == 0
        server.available = True
        assert client.flush(5).successful
        assert len(_all_events(server)) == 10
    finally:
        client.shutdown(1)


def test_offline_script_exit_then_fresh_process_sync(mock_server, monkeypatch):
    url, server = mock_server
    server.available = False
    monkeypatch.setenv("FEED_API_KEY", "secret")
    script = """
import dataclasses, json, feed, sys
run = feed.init("Project/training", server_url=sys.argv[1])
for step in range(5):
    assert run.log("metric", {"step": step})
print(json.dumps({"id": run.id, "report": dataclasses.asdict(run.finish(.3))}))
"""
    process = subprocess.run(
        [sys.executable, "-c", script, url], capture_output=True, text=True, timeout=5
    )
    assert process.returncode == 0, process.stderr
    saved = json.loads(process.stdout)
    assert saved["report"]["persisted_pending"] == 6
    assert saved["report"]["unsaved"] == 0
    assert status()["pending"] == 6
    server.available = True
    code, report = cli_sync()
    assert code == 0, report
    assert report["delivered"] == 6
    assert {batch["session_id"] for batch in server.received} == {saved["id"]}
    attempts = len(server.attempts)
    assert cli_sync()[0] == 0
    assert len(server.attempts) == attempts
    assert status()["pending"] == 0


def test_init_recovers_only_selected_project_and_sync_recovers_the_rest(
    mock_server, monkeypatch
):
    url, server = mock_server
    server.available = False
    monkeypatch.setenv("FEED_API_KEY", "secret")
    old = {}
    for project in ("A", "B"):
        run = init(f"{project}/training", server_url=url)
        assert run.log("metric", {"project": project})
        old[project] = run.id
        assert run.finish(0.15).persisted_pending == 2
    server.available = True
    current = init("A/training", server_url=url)
    try:
        until(lambda: any(batch["session_id"] == old["A"] for batch in server.received))
        assert not any(batch["session_id"] == old["B"] for batch in server.received)
    finally:
        current.finish(2)
    code, report = cli_sync()
    assert code == 0, report
    assert any(batch["session_id"] == old["B"] for batch in server.received)


def test_sync_continues_after_a_blocked_destination(mock_server, monkeypatch):
    url, server = mock_server
    monkeypatch.setenv("FEED_API_KEY", "secret")
    server.available = False
    for endpoint in ("blocked", "healthy"):
        run = init(f"Project/{endpoint}", server_url=url)
        assert run.log({"endpoint": endpoint})
        run.finish(0.15)
    server.available = True
    server.responder = lambda path, _batch: 403 if "/blocked/" in path else 200
    code, report = cli_sync()
    assert code == 1
    assert report["delivered"] == 2
    assert report["pending"] == 2
    assert report["errors"]
    assert any(e["data"].get("endpoint") == "healthy" for e in _all_events(server))


@pytest.mark.parametrize("payload", ["x" * 10000, "\U0001f986" * 1000, [[1] * 2000]])
def test_oversized_input_is_rejected_and_later_metrics_deliver(
    mock_server, payload, caplog
):
    url, server = mock_server
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        max_event_bytes=1024,
        memory_queue_bytes=1024**2,
    )
    try:
        assert not run.log("large", {"payload": payload})
        started = time.monotonic()
        assert not run.log_wait("large", {"payload": payload}, timeout=30)
        assert time.monotonic() - started < 1
        assert run.log("metric", {"loss": 0.5})
        report = run.finish(5)
        assert report.successful
        assert report.accepted == 2
        assert "max_event_bytes" in caplog.text
        assert any(e["data"].get("loss") == 0.5 for e in _all_events(server))
    finally:
        run.finish(1)


def test_server_rejected_large_record_is_retained_and_metrics_deliver(
    mock_server, monkeypatch
):
    url, server = mock_server
    server.responder = lambda _path, batch: (
        413 if any("blob" in e["data"] for e in batch["events"]) else 200
    )
    monkeypatch.setenv("FEED_API_KEY", "secret")
    run = init("Project/training", server_url=url, max_event_bytes=2048)
    assert run.log("large", {"blob": "x" * 1000})
    for step in range(8):
        assert run.log("metric", {"step": step})
    report = run.finish(5)
    assert report.delivered == 9
    assert report.failed == 1
    assert sorted(
        e["data"]["step"] for e in _all_events(server) if "step" in e["data"]
    ) == list(range(8))
    assert status()["failed"] == 1
    # An explicit sync can recover a formerly permanent rejection after the
    # server's limit changes, without resending acknowledged metrics.
    server.responder = None
    result = sync_spools()
    assert result["delivered"] == 1
    assert result["failed"] == result["pending"] == 0


def test_slow_large_upload_does_not_block_later_small_metrics(mock_server):
    url, server = mock_server
    entered, release = threading.Event(), threading.Event()

    def respond(_path, batch):
        if any("blob" in e["data"] for e in batch["events"]):
            entered.set()
            assert release.wait(5)
        return 200

    server.responder = respond
    run = init(
        "Project/training", server_url=url, api_key="secret", max_event_bytes=8192
    )
    try:
        assert run.log("large", {"blob": "x" * 4000})
        run.flush(0.01)
        assert entered.wait(2)
        for step in range(8):
            assert run.log("metric", {"step": step})
        report = run.flush(0.5)
        assert report.pending == 1
        assert sorted(
            e["data"]["step"] for e in _all_events(server) if "step" in e["data"]
        ) == list(range(8))
    finally:
        release.set()
        assert run.finish(5).successful


def test_queued_bytes_trigger_persistence_before_interval(mock_server):
    url, _ = mock_server
    client = Client(
        Config(
            url,
            "training",
            persist_interval_seconds=30,
            persist_threshold_bytes=1024,
            channels=[
                ChannelSettings(
                    "default", flush_threshold_events=100, flush_interval_seconds=30
                )
            ],
        )
    )
    try:
        until(lambda: client.worker_state.value == "running")
        assert client.emit("metric", EventBuilder().add("value", 1).build())
        assert client._admission.used < 1024
        # Give a complete worker tick to observe the below-threshold record.
        threading.Event().wait(0.1)
        assert client._worker._spool.counts()["pending"] == 0
        assert client.emit("large", EventBuilder().add("blob", "x" * 1500).build())
        until(lambda: client._worker._spool.counts()["pending"] == 2)
    finally:
        client.shutdown(5)


def test_stalled_storage_bounds_admission_and_reports_unsaved(mock_server, monkeypatch):
    url, _ = mock_server
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        max_event_bytes=1024,
        memory_queue_bytes=4096,
        persist_threshold_bytes=1,
    )
    assert run.flush(5).successful
    entered, release = threading.Event(), threading.Event()
    spool = run._client._worker._spool
    original = spool.persist

    def stalled(*args):
        entered.set()
        assert release.wait(5)
        original(*args)

    monkeypatch.setattr(spool, "persist", stalled)
    try:
        assert run.log({"blob": "x" * 500})
        assert entered.wait(2)
        accepted = 1
        while accepted < 100 and run.log({"blob": "x" * 500}):
            accepted += 1
        assert accepted < 100
        assert run._client._admission.used <= 4096
        report = run.finish(0.05)
        assert report.unsaved == accepted
        assert not report.successful
    finally:
        release.set()
        until(lambda: run._client.worker_state.value == "finished")


def test_lost_acknowledgement_retries_original_identity_and_payload(mock_server):
    url, server = mock_server
    client = Client(
        Config(
            url, "training", retry_base_delay_seconds=0.01, retry_max_delay_seconds=0.01
        )
    )
    try:
        until(lambda: client.worker_state.value == "running")
        server.disconnect_after_accept = True
        assert client.emit("metric", EventBuilder().add("step", 1).build())
        assert client.flush(5).successful
        assert len(server.received) == 2
        assert server.received[0] == server.received[1]
    finally:
        client.shutdown(1)


def test_crash_after_persistence_recovers_without_finish(mock_server, monkeypatch):
    url, server = mock_server
    server.available = False
    monkeypatch.setenv("FEED_API_KEY", "secret")
    script = """
import feed, os, sys
run = feed.init("Project/training", server_url=sys.argv[1])
assert run.log({"step": 1})
assert run.flush(.2).persisted_pending == 2
os._exit(0)
"""
    child = subprocess.run(
        [sys.executable, "-c", script, url], capture_output=True, timeout=5
    )
    assert child.returncode == 0, child.stderr
    server.available = True
    code, report = cli_sync()
    assert code == 0, report
    assert report["delivered"] == 2


def test_transient_disk_failure_retains_unsaved_and_recovers(mock_server, monkeypatch):
    url, server = mock_server
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        persist_interval_seconds=0.01,
    )
    assert run.flush(5).successful
    spool = run._client._worker._spool
    original = spool.persist
    fail = threading.Event()
    fail.set()

    def persist(*args):
        if fail.is_set():
            raise OSError("disk unavailable")
        return original(*args)

    monkeypatch.setattr(spool, "persist", persist)
    try:
        assert run.log({"step": 1})
        until(lambda: bool(run._client._admission.error))
        assert not run.log({"step": 2})
        report = run.flush(0.02)
        assert report.unsaved == 1
        assert report.storage_error == "disk unavailable"
        fail.clear()
        assert run.flush(5).successful
        assert run.log({"step": 3})
        assert run.finish(5).successful
        assert sorted(
            e["data"]["step"] for e in _all_events(server) if "step" in e["data"]
        ) == [1, 3]
    finally:
        fail.clear()
        run.finish(1)


def test_ack_cleanup_failure_recovers_without_double_delivery_counts(
    mock_server, monkeypatch
):
    import feed.spool as storage

    url, _ = mock_server
    run = init(
        "Project/training",
        server_url=url,
        api_key="secret",
        persist_interval_seconds=0.01,
    )
    assert run.flush(5).successful
    spool = run._client._worker._spool
    fail, observed = threading.Event(), threading.Event()
    fail.set()
    original = storage._sync_directory

    def sync_directory(path):
        if (
            path == spool.path / "data"
            and fail.is_set()
            and not list(path.glob("*.event"))
        ):
            observed.set()
            raise OSError("cannot sync acknowledgement cleanup")
        return original(path)

    monkeypatch.setattr(storage, "_sync_directory", sync_directory)
    try:
        assert run.log({"step": 1})
        run.flush(0.1)
        assert observed.wait(2)
        assert run._client.is_running
        fail.clear()
        until(lambda: not run._client._admission.error)
        assert run.log({"step": 2})
        report = run.finish(5)
        assert report.successful
        assert report.delivered in (1, 2)  # first flush may already report step 1
        assert report.pending == 0
    finally:
        fail.clear()
        run.finish(1)

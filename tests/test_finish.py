"""Delivery completion and cancellation at client exit."""

import json
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import feed
from feed.sync import status
from test_integration import _all_events
from test_recovery import until


MESSAGE = "[feed] Syncing remaining feeds, cancel with Ctrl+C."


def client_for(url, **kwargs):
    return feed.Client(
        feed.Config(
            url,
            "training",
            client_secret="secret",
            retry_base_delay_seconds=0.001,
            retry_max_delay_seconds=0.005,
            **kwargs,
        )
    )


def test_finish_waits_through_an_outage_beyond_the_default_deadline(
    mock_server, monkeypatch, capsys
):
    import feed.worker as worker

    url, server = mock_server
    server.available = False
    elapsed = [0]
    monkeypatch.setattr(
        worker, "time", SimpleNamespace(monotonic=lambda: time.monotonic() + elapsed[0])
    )
    client = client_for(url)
    assert capsys.readouterr().out == f"[feed] Session: {client.id}\n"
    reports = []
    completed = threading.Event()

    def finish():
        try:
            reports.append(client.finish())
        finally:
            completed.set()

    assert client.log("readings", {"value": 1})
    thread = threading.Thread(target=finish)
    thread.start()
    try:
        until(lambda: status()["pending"] == 1)
        elapsed[0] = 60
        client._wake.set()
        assert not completed.wait(0.2)
        assert capsys.readouterr().out == MESSAGE + "\n"
        server.available = True
        assert completed.wait(5)
        assert reports[0].successful and reports[0].delivered == 1
        assert status()["pending"] == 0
        assert capsys.readouterr().out == "[feed] Complete: 1 delivered.\n"
    finally:
        server.available = True
        client.shutdown(0)
        thread.join(5)


def test_context_exit_drains_all_matching_recovery_sessions(mock_server, capsys):
    url, server = mock_server
    server.available = False
    sessions = set()
    for value in range(3):
        previous = client_for(url, max_concurrent_requests=1)
        assert previous.log("readings", {"value": value})
        assert previous.finish(0.2).persisted_pending == 1
        sessions.add(previous.id)
    other = feed.init(ingest_url=f"{url}/v1/other", api_key="secret")
    assert other.log("readings", {"value": 99})
    assert other.finish(0.2).persisted_pending == 1
    capsys.readouterr()

    server.available = True
    with client_for(url, max_concurrent_requests=1) as client:
        pass

    assert {batch["session_id"] for batch in server.received} == sessions
    assert sorted(e["data"]["value"] for e in _all_events(server)) == [0, 1, 2]
    assert status()["pending"] == 1
    assert capsys.readouterr().out.splitlines() == [
        f"[feed] Session: {client.id}",
        MESSAGE,
        "[feed] Complete: 0 delivered.",
    ]


def test_explicit_finish_timeout_keeps_pending_records(mock_server, capsys):
    url, server = mock_server
    server.available = False
    client = client_for(url)
    assert client.log("readings", {"value": 1})
    started = time.monotonic()
    report = client.finish(timeout=0.2)
    assert time.monotonic() - started < 2
    assert report.timed_out and report.persisted_pending == 1
    assert status()["pending"] == 1
    assert capsys.readouterr().out.splitlines() == [
        f"[feed] Session: {client.id}",
        MESSAGE,
        "[feed] Incomplete: 0 delivered, 1 pending on disk.",
    ]


def test_finish_reports_permanent_rejection_without_waiting_forever(mock_server, capsys):
    url, server = mock_server
    server.response_statuses = [400]
    client = client_for(url)
    assert client.log("readings", {"value": 1})
    report = client.finish()
    assert not report.successful
    assert report.complete and report.failed == 1
    assert status()["failed"] == 1
    assert capsys.readouterr().out.splitlines()[-1] == (
        "[feed] Incomplete: 0 delivered, 1 failed."
    )


def test_empty_session_reports_zero_counts_and_disabled_client_is_quiet(mock_server, capsys):
    url, _ = mock_server
    client = client_for(url)
    assert client.finish().successful
    assert capsys.readouterr().out.splitlines() == [
        f"[feed] Session: {client.id}",
        "[feed] Complete: 0 delivered.",
    ]
    with feed.init(enabled=False) as disabled:
        assert disabled.finish().successful
    assert capsys.readouterr().out == ""


def test_finish_prints_session_totals_including_earlier_flushes(mock_server, capsys):
    url, _ = mock_server
    client = feed.init(ingest_url=f"{url}/v1/readings", api_key="secret")
    assert capsys.readouterr().out == f"[feed] Session: {client.id}\n"
    for value in range(2):
        assert client.log("readings", {"value": value})
        assert client.flush(5).delivered == 1
    assert capsys.readouterr().out == ""
    assert client.log("readings", {"value": 2})
    assert client.finish().delivered == 1
    assert capsys.readouterr().out.splitlines()[-1] == (
        "[feed] Complete: 3 delivered."
    )


@pytest.mark.parametrize("interruption", ["finish", "context_exit", "body"])
def test_interrupt_cancels_wait_and_releases_durable_spool(mock_server, interruption):
    url, server = mock_server
    server.available = False
    script = '''
import json, signal, sys, threading, time
import feed
from feed.spool import SpoolRoot

client = feed.init(ingest_url=sys.argv[1] + "/v1/training", api_key="secret",
                   persist_interval_seconds=0.001)
mode = sys.argv[2]

def produce():
    assert client.log("readings", {"value": 1})
    deadline = time.monotonic() + 2
    while not client._worker._spool.counts()["pending"]:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    if mode == "body":
        raise KeyboardInterrupt
    threading.Timer(0.2, lambda: signal.raise_signal(signal.SIGINT)).start()
    if mode == "finish":
        client.finish()

try:
    if mode == "finish":
        produce()
    else:
        with client:
            produce()
except KeyboardInterrupt:
    deadline = time.monotonic() + 2
    while client.is_running:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    recovered = SpoolRoot().claim(client._worker._spool.path)
    assert recovered is not None
    print(json.dumps(recovered.counts()), flush=True)
    recovered.close()
else:
    raise AssertionError("KeyboardInterrupt did not propagate")
'''
    process = subprocess.run(
        [sys.executable, "-c", script, url, interruption],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert process.returncode == 0, process.stderr
    lines = process.stdout.splitlines()
    assert json.loads(lines[-1]) == {"pending": 1, "failed": 0}
    assert lines[0].startswith("[feed] Session: ")
    assert lines[1:-2] == ([] if interruption == "body" else [MESSAGE])
    assert lines[-2] == (
        "[feed] Cancelled: 0 delivered, 1 pending on disk."
    )

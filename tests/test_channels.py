import threading

from feed.client import Client
from feed.config import ChannelSettings, Config
from feed.fields import EventBuilder
from feed.worker import Worker
from test_integration import _all_events
from test_recovery import until


def test_persisted_uploads_keep_channel_priority(mock_server, monkeypatch):
    url, server = mock_server
    with monkeypatch.context() as patch:
        patch.setattr(Worker, "start", lambda _self: None)
        client = Client(
            Config(
                url,
                "training",
                max_concurrent_requests=1,
                channels=[
                    ChannelSettings("low", priority=5),
                    ChannelSettings("high", priority=-5),
                ],
            )
        )
        for name in ("low", "high"):
            assert client.emit_on(
                client.channel(name), "metric", EventBuilder().add("value", 1).build()
            )
    client._worker.start()
    try:
        assert client.flush(5).successful
        assert [e["channel"] for e in _all_events(server)] == ["high", "low"]
    finally:
        client.shutdown(1)


def test_global_and_per_channel_upload_slots_are_preserved(mock_server):
    url, server = mock_server
    release = threading.Event()
    lock = threading.Lock()
    active = {"a": 0, "b": 0}
    peaks = {"a": 0, "b": 0, "total": 0}

    def respond(_path, batch):
        channel = batch["events"][0]["channel"]
        with lock:
            active[channel] += 1
            peaks[channel] = max(peaks[channel], active[channel])
            peaks["total"] = max(peaks["total"], sum(active.values()))
        assert release.wait(5)
        with lock:
            active[channel] -= 1
        return 200

    server.responder = respond
    client = Client(
        Config(
            url,
            "training",
            max_concurrent_requests=3,
            channels=[
                ChannelSettings("a", max_concurrent_slots=1, flush_threshold_events=1),
                ChannelSettings("b", max_concurrent_slots=2, flush_threshold_events=1),
            ],
        )
    )
    try:
        for name in ("a", "b"):
            for value in range(3):
                assert client.emit_on(
                    client.channel(name),
                    "metric",
                    EventBuilder().add("value", value).build(),
                )
        until(lambda: sum(active.values()) == 3)
        assert peaks == {"a": 1, "b": 2, "total": 3}
    finally:
        release.set()
        assert client.shutdown(5).successful


def test_rate_rejection_does_not_consume_channel_sequence(mock_server):
    url, server = mock_server
    client = Client(
        Config(
            url,
            "training",
            channels=[ChannelSettings("default", max_events_per_second=1)],
        )
    )
    try:
        assert client.emit("metric", EventBuilder().add("value", 1).build())
        assert not client.emit("metric", EventBuilder().add("value", 2).build())
        client._handles[0]._rate._tokens = 1
        assert client.emit("metric", EventBuilder().add("value", 3).build())
        assert client.flush(5).successful
        assert [e["session_sequence_num"] for e in _all_events(server)] == [0, 1]
    finally:
        client.shutdown(1)


def test_flush_does_not_disable_later_channel_batching(mock_server):
    url, server = mock_server
    client = Client(
        Config(
            url,
            "training",
            persist_threshold_bytes=1,
            channels=[
                ChannelSettings(
                    "default", flush_threshold_events=100, flush_interval_seconds=30
                )
            ],
        )
    )
    try:
        assert client.emit("metric", EventBuilder().add("step", 1).build())
        assert client.flush(5).successful
        until(lambda: not client._worker._current.force)
        assert client.emit("metric", EventBuilder().add("step", 2).build())
        until(lambda: client._worker._spool.counts()["pending"] == 1)
        threading.Event().wait(0.1)
        assert len(_all_events(server)) == 1
        assert client.flush(5).successful
        assert len(_all_events(server)) == 2
    finally:
        client.shutdown(1)

"""End-to-end tests against a real (in-process) HTTP server.

Exercises the full worker path: blacklist fetch, emit -> merge -> hash -> batch
-> gzip -> upload, response parsing, blacklist filtering, and shutdown flush.
"""

import gzip
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from feed import init
from feed.client import Client
from feed.config import ChannelSettings, Config
from feed.fields import EventBuilder
from feed.schema import compute_schema_hash


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence
        pass

    def _respond(self, code, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.endswith("/blacklist"):
            with self.server.lock:
                self.server.auth_headers.append(self.headers.get("Authorization"))
            body = json.dumps({"rules": self.server.blacklist_rules}).encode()
            self._respond(200, body)
        else:
            self._respond(404, b"{}")

    def do_POST(self):
        if self.path.endswith("/telemetry"):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            if self.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            batch = json.loads(raw)
            with self.server.lock:
                self.server.attempts.append(batch)
                response_status = (
                    self.server.response_statuses.pop(0)
                    if self.server.response_statuses
                    else 200
                )
            if response_status != 200:
                self._respond(response_status, b"{}")
                return
            max_events = self.server.max_events_per_request
            if max_events is not None and len(batch.get("events", [])) > max_events:
                self._respond(413, b"{}")
                return
            with self.server.lock:
                self.server.received.append(batch)
                self.server.auth_headers.append(self.headers.get("Authorization"))
            n = len(batch.get("events", []))
            self._respond(200, json.dumps({"ingested": n, "dropped": 0}).encode())
        else:
            self._respond(404, b"{}")


@pytest.fixture
def mock_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.received = []
    server.attempts = []
    server.response_statuses = []
    server.blacklist_rules = []
    server.auth_headers = []
    server.max_events_per_request = None
    server.lock = threading.Lock()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    yield f"http://{host}:{port}", server
    server.shutdown()
    server.server_close()


def _all_events(server):
    with server.lock:
        return [ev for batch in server.received for ev in batch["events"]]


def test_emits_and_uploads_batch(mock_server):
    url, server = mock_server
    client = Client(Config(url, "project-id", client_secret="s3cr3t"))

    client.set_state("player_id", "acct_42")
    for i in range(3):
        assert client.emit(
            "enemy_killed",
            EventBuilder().add("enemy_type", "goblin").add("wave", i).build(),
        )

    client.shutdown(5.0)

    events = _all_events(server)
    assert len(events) == 3
    first_batch = server.received[0]
    assert isinstance(first_batch["session_id"], str)
    ev = events[0]
    assert ev["channel"] == "default"
    assert ev["data"]["player_id"] == "acct_42"
    assert ev["data"]["enemy_type"] == "goblin"
    schema_def = first_batch["schemas"][ev["schema_hash"]]
    assert schema_def["$schema_name"] == "enemy_killed"
    assert schema_def["player_id"] == "string"
    assert schema_def["wave"] == "int64"


def test_flush_delivers_without_stopping_the_worker(mock_server):
    url, server = mock_server
    client = Client(
        Config(
            url,
            "project-id",
            channels=[
                ChannelSettings(
                    "bulk",
                    flush_threshold_events=100,
                    flush_interval_seconds=60.0,
                )
            ],
        )
    )
    bulk = client.channel("bulk")

    assert client.emit_on(bulk, "metric", EventBuilder().add("value", 1).build())
    first = client.flush(5.0)
    assert first.successful
    assert first.accepted == 1
    assert first.delivered == 1
    assert client.is_running

    assert client.emit_on(bulk, "metric", EventBuilder().add("value", 2).build())
    second = client.flush(5.0)
    assert second.successful
    assert second.accepted == 1
    assert second.delivered == 1
    assert [event["data"]["value"] for event in _all_events(server)] == [1, 2]

    final = client.shutdown(5.0)
    assert final.successful


def test_flush_timeout_keeps_original_identity_for_retry(mock_server):
    url, server = mock_server
    server.response_statuses = [503]
    client = Client(
        Config(
            url,
            "project-id",
            channels=[
                ChannelSettings(
                    "bulk",
                    flush_threshold_events=100,
                    flush_interval_seconds=60.0,
                )
            ],
            max_retries=0,
            retry_base_delay_seconds=0.01,
            retry_max_delay_seconds=0.01,
        )
    )
    bulk = client.channel("bulk")
    assert client.emit_on(bulk, "metric", EventBuilder().add("value", 1).build())

    first = client.flush(0.001)
    assert not first.complete
    assert first.pending == 1

    second = client.flush(5.0)
    assert second.successful
    assert second.delivered == 1
    with server.lock:
        identities = [
            (
                batch["session_id"],
                batch["events"][0]["channel"],
                batch["events"][0]["session_sequence_num"],
            )
            for batch in server.attempts
        ]
    assert len(identities) >= 2
    assert len(set(identities)) == 1
    client.shutdown(5.0)


def test_flush_reports_terminal_drop(mock_server):
    url, server = mock_server
    server.response_statuses = [400]
    client = Client(Config(url, "project-id"))
    assert client.emit("metric", EventBuilder().add("value", 1).build())

    report = client.flush(5.0)
    assert report.complete
    assert not report.successful
    assert report.dropped == 1
    assert report.pending == 0
    client.shutdown(5.0)


def test_bearer_provider_is_used_for_blacklist_and_upload(mock_server):
    url, server = mock_server
    client = Client(
        Config(url, "project-id", bearer_token_provider=lambda: "current-user-token")
    )
    assert client.emit("metric", EventBuilder().add("value", 1.0).build())
    client.shutdown(5.0)

    with server.lock:
        assert server.auth_headers
        assert set(server.auth_headers) == {"Bearer current-user-token"}


def test_oversized_batch_is_split_until_the_server_accepts_it(mock_server, caplog):
    url, server = mock_server
    caplog.set_level("INFO", logger="feed")
    server.max_events_per_request = 3
    client = Client(
        Config(
            url,
            "project-id",
            channels=[
                ChannelSettings(
                    "stress",
                    flush_threshold_events=8,
                    flush_interval_seconds=60.0,
                )
            ],
        )
    )
    stress = client.channel("stress")

    for ordinal in range(8):
        assert client.emit_on(
            stress,
            "stress_event",
            EventBuilder().add("ordinal", ordinal).build(),
        )
    client.shutdown(5.0)

    events = _all_events(server)
    assert sorted(event["data"]["ordinal"] for event in events) == list(range(8))
    assert all(len(batch["events"]) <= 3 for batch in server.received)
    assert "splitting oversized batch events=8 into 4 and 4" in caplog.text


def test_blacklisted_events_dropped_clientside(mock_server):
    url, server = mock_server
    server.blacklist_rules = [
        {"schema_hash": compute_schema_hash({"$schema_name": "noise", "spam": "int64"})}
    ]
    client = Client(Config(url, "project-id"))

    for i in range(5):
        client.emit("noise", EventBuilder().add("spam", i).build())
    client.emit("signal", EventBuilder().add("value", 1).build())

    client.shutdown(5.0)

    names = []
    for batch in server.received:
        for ev in batch["events"]:
            names.append(batch["schemas"][ev["schema_hash"]]["$schema_name"])
    assert names == ["signal"]


def test_names_lowercased_and_case_insensitive_on_wire(mock_server):
    url, server = mock_server
    client = Client(Config(url, "project-id", channels=[ChannelSettings("Signals")]))

    client.set_state("Sample_Id", "sample_7")
    signals = client.channel("SIGNALS")  # looked up case-insensitively
    assert client.emit_on(
        signals,
        "SampleRecorded",
        EventBuilder()
        .add("sample_id", "sample_OVERRIDE")
        .add("SensorName", "sensor-a")
        .build(),
    )

    client.shutdown(5.0)

    events = _all_events(server)
    assert len(events) == 1
    ev = events[0]
    assert ev["channel"] == "signals"
    schema_def = server.received[0]["schemas"][ev["schema_hash"]]
    assert schema_def["$schema_name"] == "samplerecorded"
    assert "sensorname" in schema_def
    # Sample_Id state + sample_id event collapse to one column, event wins.
    assert ev["data"]["sample_id"] == "sample_OVERRIDE"
    assert sum(1 for k in ev["data"] if k == "sample_id") == 1


def test_run_logs_map_names_and_record_shapes_to_wire_schemas(mock_server):
    url, server = mock_server
    run = init(
        project="research-project",
        server_url=url,
        api_key="secret",
        name="baseline",
        config={"Model": {"Width": 64, "Dropout": 0.1}},
        tags=["paper"],
    )
    assert run.log("train", {"step": 10, "loss": 1.0})
    assert run.log("train", {"step": 10, "accuracy": 0.25})
    assert run.log("train", {"step": 3, "loss": 0.9})
    assert run.log("train", {"step": 4, "loss": 0.8})
    assert run.log({"wall_clock_loss": 0.8})
    assert run.log("validation", {"step": 3, "checkpoint": "best", "val_loss": 0.7})
    assert run.log("test", {"item_id": "aggregate", "accuracy": 0.8})
    assert run.log(
        "custom_analysis",
        {"matrix": [[1.0, 2.0], [3.0, 4.0]], "summary": {"mean": 2.5}},
    )
    assert run.log_wait("export", {"records": 500}, timeout=1.0)
    run.finish(5.0)

    batches = list(server.received)
    events = _all_events(server)
    schemas = {
        schema_hash: definition
        for batch in batches
        for schema_hash, definition in batch["schemas"].items()
    }
    train_events = [
        event
        for event in events
        if schemas[event["schema_hash"]]["$schema_name"] == "train"
    ]
    assert len(train_events) == 4
    assert {event["data"]["step"] for event in train_events} == {10, 3, 4}
    assert {event["channel"] for event in train_events} == {"data"}
    assert len({event["schema_hash"] for event in train_events}) == 2
    assert {
        frozenset(schemas[event["schema_hash"]].keys()) for event in train_events
    } == {
        frozenset(("$schema_name", "step", "loss")),
        frozenset(("$schema_name", "step", "accuracy")),
    }

    default_log = next(
        event
        for event in events
        if schemas[event["schema_hash"]]["$schema_name"] == "log"
    )
    assert default_log["data"] == {"wall_clock_loss": 0.8}

    run_event = next(
        event
        for event in events
        if schemas[event["schema_hash"]]["$schema_name"] == "run"
    )
    assert run_event["data"]["config"] == {"Model": {"Width": 64, "Dropout": 0.1}}
    assert schemas[run_event["schema_hash"]]["config"] == "variant"
    custom = next(
        event
        for event in events
        if schemas[event["schema_hash"]]["$schema_name"] == "custom_analysis"
    )
    assert custom["data"]["matrix"] == [[1.0, 2.0], [3.0, 4.0]]
    export = next(
        event
        for event in events
        if schemas[event["schema_hash"]]["$schema_name"] == "export"
    )
    assert export["data"] == {"records": 500}


def test_run_rejects_invalid_stream_names_before_queueing(mock_server):
    url, _ = mock_server
    run = init(project="research-project", server_url=url, api_key="secret")

    with pytest.raises(ValueError, match="stream name"):
        run.log("train metrics", {"loss": 1.0})

    report = run.finish(5.0)
    assert report.successful

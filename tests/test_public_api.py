"""The basic event API and dictionary helpers share one client."""

import runpy
import uuid
from collections import Counter
from pathlib import Path

import pytest
import requests

import feed
from feed.client import Client
from test_integration import _all_events
from test_recovery import until


def test_ingest_url_routes_upload_and_blacklist(mock_server, monkeypatch):
    url, server = mock_server
    calls = []
    request = requests.Session.request

    def capture(session, method, target, **kwargs):
        calls.append((method, target, kwargs["headers"].get("X-Client-Secret")))
        return request(session, method, target, **kwargs)

    monkeypatch.setattr(requests.Session, "request", capture)
    monkeypatch.setenv("FEED", "unrelated/feed")
    monkeypatch.setenv("FEED_URL", "https://unrelated.test")
    monkeypatch.setenv("FEED_INGEST_URL", "https://unrelated.test/v1/other/telemetry")
    monkeypatch.setenv("FEED_API_KEY", "other-key")
    with feed.init(ingest_url=f"{url}/proxy/v1/training", api_key="secret") as client:
        assert client.feed == "training"
        assert client.log("readings", {"temperature": 21.4})
        assert client.flush(5).delivered == 1

    assert ("GET", f"{url}/proxy/v1/training/blacklist", "secret") in calls
    assert ("POST", f"{url}/proxy/v1/training/telemetry", "secret") in calls
    assert _all_events(server)[0]["data"] == {"temperature": 21.4}


def test_ingest_url_recovers_saved_events(mock_server):
    url, server = mock_server
    server.available = False
    old = feed.init(ingest_url=f"{url}/v1/training/", api_key="secret")
    assert old.log("readings", {"temperature": 21.4})
    assert old.finish(0.2).persisted_pending == 1
    server.available = True
    with feed.init(
        ingest_url=f"{url}/v1/training/telemetry", api_key="secret"
    ) as client:
        until(lambda: len(_all_events(server)) == 1)
        assert client.flush().accepted == 0
    assert {batch["session_id"] for batch in server.received} == {old.session_id}


def test_init_creates_no_records_or_special_channels(mock_server):
    url, server = mock_server
    client = feed.init("Project/training", server_url=url, api_key="secret")
    try:
        assert isinstance(client, Client)
        assert client.id == client.session_id
        uuid.UUID(client.session_id)
        assert client.feed == "Project/training"
        assert [settings.name for settings in client._config.channels] == []
        assert client.flush(5).accepted == 0
        assert _all_events(server) == []
    finally:
        assert client.finish(5).successful


@pytest.mark.parametrize(
    "name,value",
    [("name", "baseline"), ("config", {}), ("tags", []), ("group", "ablation")],
)
def test_init_metadata_must_be_an_explicit_record(name, value):
    with pytest.raises(TypeError, match=name):
        feed.init(enabled=False, **{name: value})


def test_public_types_are_available_without_internal_imports():
    for name in (
        "Client",
        "Config",
        "Channel",
        "ChannelSettings",
        "EventBuilder",
        "Field",
        "FieldType",
        "WorkerState",
        "ConfigError",
    ):
        assert name in feed.__all__
        assert getattr(feed, name) is not None
    assert feed.Run is feed.Client  # Existing type imports remain usable.


@pytest.mark.parametrize("direct", [False, True])
def test_emit_and_log_share_state_and_session(mock_server, direct):
    url, server = mock_server
    client = (
        feed.Client(feed.Config(url, "training"))
        if direct
        else feed.init("Project/training", server_url=url, api_key="secret")
    )
    with client:
        client.set_state("Experiment", "baseline")
        client.set_state_optional_int("seed", None)
        client.set_state_string_array("labels", [])
        assert client.has_state("EXPERIMENT")
        assert client.emit("observations", feed.EventBuilder().add("value", 1).build())
        client.set_state("experiment", "changed")
        assert client.log("observations", {"experiment": "event", "value": 2})
        client.remove_state("experiment")
        assert not client.has_state("experiment")
        assert client.log_wait({"value": 3}, timeout=1)
        report = client.flush(5)
        assert report.accepted == report.delivered == 3
        assert client.is_running
    assert not client.is_running
    assert client.finish(0).successful
    assert not client.log({"value": 4})
    assert not client.emit("observations", [])
    assert {batch["session_id"] for batch in server.received} == {client.id}
    events = sorted(_all_events(server), key=lambda e: e["session_sequence_num"])
    assert [e["data"].get("experiment") for e in events] == ["baseline", "event", None]
    assert all(e["data"]["seed"] is None and e["data"]["labels"] == [] for e in events)
    assert {e["channel"] for e in events} == {"default"}


def test_run_metadata_is_an_ordinary_user_named_event(mock_server):
    url, server = mock_server
    with feed.init("Project/training", server_url=url, api_key="secret") as client:
        assert client.log("runs", {"name": "plain"})
        assert client.emit(
            "experiments",
            feed.EventBuilder()
            .add_string("name", "typed")
            .add_variant("config", {"optimizer": {"lr": 0.01}, "empty": []})
            .build(),
        )
        assert client.log("train", {"step": 0, "loss": 0.5})
        assert client.flush(5).delivered == 3
    records = {
        batch["schemas"][event["schema_hash"]]["$schema_name"]: event["data"]
        for batch in server.received
        for event in batch["events"]
    }
    assert records == {
        "runs": {"name": "plain"},
        "experiments": {
            "name": "typed",
            "config": {"optimizer": {"lr": 0.01}, "empty": []},
        },
        "train": {"step": 0, "loss": 0.5},
    }
    assert any(
        schema.get("config") == "variant"
        for batch in server.received
        for schema in batch["schemas"].values()
    )


def test_dictionary_state_and_arrays_keep_types(mock_server):
    url, server = mock_server
    with feed.init(ingest_url=f"{url}/v1/sensors", api_key="secret") as client:
        client.set_state("device", {"Name": "room_1"})
        client.set_state("ranges", [{"Start": 1, "End": 3}])
        assert client.log("readings", {"temperature": 21.4})
        client.set_state("device", {"Index": 2})
        assert client.log("readings", {"temperature": 21.5})
        assert client.flush(5).delivered == 2

    events = sorted(
        _all_events(server), key=lambda event: event["session_sequence_num"]
    )
    schemas = {
        schema_hash: schema
        for batch in server.received
        for schema_hash, schema in batch["schemas"].items()
    }
    assert [event["data"]["device"] for event in events] == [
        {"name": "room_1"},
        {"index": 2},
    ]
    assert len(schemas) == 2
    assert [schemas[event["schema_hash"]]["device"] for event in events] == [
        {"name": "string"},
        {"index": "int64"},
    ]
    for event in events:
        assert event["data"]["ranges"] == [{"start": 1, "end": 3}]
        assert schemas[event["schema_hash"]]["ranges"] == [
            {"start": "int64", "end": "int64"}
        ]


def test_log_keeps_arrays_typed_and_emit_keeps_structs(mock_server):
    url, server = mock_server
    with feed.init("Project/training", server_url=url, api_key="secret") as client:
        assert client.log(
            "inferred",
            {
                "config": {"Width": 64},
                "enabled": True,
                "name": "baseline",
                "loss": 0.5,
                "counts": (1, 2),
                "matrix": [[1.0], [2.0]],
                "layers": [{"Width": 64}, {"Width": 32}],
            },
        )
        assert client.emit(
            "explicit", feed.EventBuilder().add("config", {"Width": 64}).build()
        )
        assert client.flush(5).delivered == 2
    schemas = {
        schema["$schema_name"]: schema
        for batch in server.received
        for schema in batch["schemas"].values()
    }
    assert schemas["inferred"] == {
        "$schema_name": "inferred",
        "config": {"width": "int64"},
        "enabled": "bool",
        "name": "string",
        "loss": "float64",
        "counts": ["int64"],
        "matrix": [["float64"]],
        "layers": [{"width": "int64"}],
    }
    assert schemas["explicit"]["config"] == {"width": "int64"}


def test_init_accepts_explicit_channel_configuration(mock_server):
    url, server = mock_server
    with feed.init(
        "Project/training",
        server_url=url,
        api_key="secret",
        channels=[feed.ChannelSettings("priority", priority=-1)],
    ) as client:
        fields = feed.EventBuilder().add_optional_string("label", None).build()
        assert client.emit_on(client.channel("PRIORITY"), "events", fields)
        assert client.log("events", {"label": "default"})
        assert client.flush(5).delivered == 2
    assert {event["channel"] for event in _all_events(server)} == {
        "priority",
        "default",
    }


def test_disabled_basic_api_needs_no_credentials_or_payload_validation(monkeypatch):
    monkeypatch.delenv("FEED_API_KEY", raising=False)
    monkeypatch.delenv("FEED_URL", raising=False)
    with feed.init(enabled=False) as client:
        assert not client.emit("", object())
        assert not client.emit_wait("", object(), timeout=0)
        assert not client.log({"unsupported": object()})
        assert not client.log_wait({"unsupported": object()}, timeout=0)
        assert client.flush().accepted == 0
        assert client.finish().successful


def test_init_recovers_legacy_metadata_and_data_channels(mock_server):
    url, server = mock_server
    server.available = False
    old = feed.Client(
        feed.Config(
            url,
            "training",
            client_secret="secret",
            feed_reference="Project/training",
            channels=[feed.ChannelSettings("metadata"), feed.ChannelSettings("data")],
        )
    )
    assert old.emit_on(
        old.channel("metadata"), "run", feed.EventBuilder().add("name", "old").build()
    )
    assert old.emit_on(
        old.channel("data"), "train", feed.EventBuilder().add("step", 1).build()
    )
    assert old.shutdown(0.2).persisted_pending == 2
    server.available = True
    with feed.init("Project/training", server_url=url, api_key="secret") as client:
        until(lambda: len(_all_events(server)) == 2)
        assert client.flush().accepted == 0
    assert {batch["session_id"] for batch in server.received} == {old.session_id}
    assert {event["channel"] for event in _all_events(server)} == {"metadata", "data"}


@pytest.mark.parametrize(
    "example,expected_streams",
    [
        ("basic.py", {"readings": 3, "status": 1}),
        ("uv/main.py", {"devices": 1, "readings": 3, "status": 1}),
    ],
)
def test_examples_emit_only_their_explicit_records(
    mock_server, monkeypatch, example, expected_streams
):
    url, server = mock_server
    monkeypatch.setenv("FEED", "Project/training")
    monkeypatch.setenv("FEED_URL", url)
    monkeypatch.setenv("FEED_API_KEY", "secret")
    path = Path(__file__).resolve().parents[1] / "examples" / example
    runpy.run_path(str(path), run_name="__main__")
    assert (
        Counter(
            batch["schemas"][event["schema_hash"]]["$schema_name"]
            for batch in server.received
            for event in batch["events"]
        )
        == expected_streams
    )
    assert len({batch["session_id"] for batch in server.received}) == 1

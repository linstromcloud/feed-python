from unittest.mock import Mock

import pytest

from feed.config import Config
from feed.transport import Transport


EVENT = {
    "ticket": 7,
    "seq": 3,
    "channel": "data",
    "schema_hash": "metric",
    "schema_def": {"value": "int64"},
    "data": {"value": 1},
}


def transport_response(status, body):
    transport = Transport(Config("https://feed.test", "training"))
    session = Mock()
    response = Mock(status_code=status, headers={})
    response.json.return_value = body
    session.post.return_value = response
    transport.local.session = session
    return transport, session


@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        {"ingested": 0, "dropped": 0},
        {"ingested": True, "dropped": 0},
        {"ingested": 2, "dropped": -1},
    ],
)
def test_invalid_acknowledgement_retains_records(body):
    transport, _ = transport_response(200, body)
    assert transport.upload("run", [EVENT]).kind == "retry"


def test_valid_acknowledgement_and_server_filtering():
    transport, _ = transport_response(200, {"ingested": 1, "dropped": 0})
    assert transport.upload("run", [EVENT]).kind == "success"
    transport, _ = transport_response(
        200,
        {
            "ingested": 0,
            "dropped": 1,
            "blacklisted": [{"schema_hash": "metric", "match": {"value": "1"}}],
        },
    )
    outcome = transport.upload("run", [EVENT])
    assert outcome.kind == "success"
    assert outcome.filtered == {7}


@pytest.mark.parametrize(
    "value,rules",
    [
        pytest.param(
            12.0,
            [{"schema_hash": "metric", "match": {"value": "12"}}],
            id="float-text-mismatch",
        ),
        pytest.param(None, [], id="server-omits-null-rule"),
        pytest.param(
            None,
            [{"schema_hash": "metric", "match": {"value": "null"}}],
            id="client-rejects-null-match",
        ),
    ],
)
def test_unexplained_drops_are_acknowledged(value, rules, monkeypatch, caplog):
    monkeypatch.setattr("feed.transport.Blacklist.is_blacklisted", lambda *args: False)
    event = {
        **EVENT,
        "schema_def": {"value": {"$optional": "float64"}},
        "data": {"value": value},
    }
    transport, _ = transport_response(
        200, {"ingested": 0, "dropped": 1, "blacklisted": rules}
    )

    outcome = transport.upload("session", [event])

    assert outcome.kind == "success"
    assert outcome.filtered == set()
    assert len(outcome.rules) == len(rules)
    assert "unexplained server drops=1" in caplog.text


@pytest.mark.parametrize(
    "dropped,rules,filtered,unexplained",
    [
        (2, [{"schema_hash": "metric", "match": {"value": "1"}}], {1}, 1),
        (1, [{"schema_hash": "metric"}], set(), 1),
        (0, [{"schema_hash": "metric"}], set(), 0),
    ],
    ids=["partial-attribution", "excess-attribution", "no-server-drops"],
)
def test_inconsistent_drop_attribution(dropped, rules, filtered, unexplained, caplog):
    events = [
        {**EVENT, "ticket": value, "seq": value, "data": {"value": value}}
        for value in (1, 2, 3)
    ]
    transport, _ = transport_response(
        200, {"ingested": 3 - dropped, "dropped": dropped, "blacklisted": rules}
    )

    outcome = transport.upload("session", events)

    assert outcome.kind == "success"
    assert outcome.filtered == filtered
    assert len(outcome.rules) == len(rules)
    assert f"unexplained server drops={unexplained}" in caplog.text


def test_telemetry_does_not_follow_redirects():
    transport, session = transport_response(302, {})
    assert transport.upload("run", [EVENT]).kind == "retry"
    assert session.post.call_args.kwargs["allow_redirects"] is False

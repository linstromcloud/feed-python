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


def test_telemetry_does_not_follow_redirects():
    transport, session = transport_response(302, {})
    assert transport.upload("run", [EVENT]).kind == "retry"
    assert session.post.call_args.kwargs["allow_redirects"] is False

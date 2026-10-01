import pytest

import feed.run as run_module
from feed import init


def test_disabled_run_needs_no_credentials_or_endpoint(monkeypatch):
    monkeypatch.delenv("FEED_API_KEY", raising=False)
    monkeypatch.delenv("FEED_URL", raising=False)
    monkeypatch.setenv("FEED_INGEST_URL", "invalid")

    def fail_authentication(*_args, **_kwargs):
        raise AssertionError("disabled runs must not authenticate")

    monkeypatch.setattr(run_module, "authenticated_feed", fail_authentication)

    run = init("Research/feed-a", enabled=False)

    assert not run.log("train", {"unsupported": object(), "missing": None})
    assert not run.log_wait("train", {"values": []}, timeout=0)
    assert run.flush().successful
    assert run.finish().successful


def test_disabled_run_still_has_an_identity():
    run = init("Research/feed-a", enabled=False)
    assert run.feed == "Research/feed-a"
    assert run.id


def test_init_without_feed_uses_authenticated_default(monkeypatch):
    captured = {}

    def authenticate(feed, server_url):
        captured["feed"] = feed
        captured["server_url"] = server_url
        return (
            "https://paper.feed.test",
            "paper",
            lambda: "access-token",
            "Research/paper",
        )

    class _Client:
        enabled = True
        session_id = "run-1"

        def __init__(self, config):
            captured["config"] = config
            self.feed = config.feed_reference

    monkeypatch.delenv("FEED", raising=False)
    monkeypatch.setattr(run_module, "authenticated_feed", authenticate)
    monkeypatch.setattr(run_module, "Client", _Client)

    run = init()

    assert captured["feed"] is None
    assert captured["config"].endpoint_id == "paper"
    assert run.feed == "Research/paper"


@pytest.fixture
def config_only_init(monkeypatch):
    for name in ("FEED", "FEED_URL", "FEED_INGEST_URL", "FEED_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(run_module, "Client", lambda config: config)

    def unexpected_login(*args):
        pytest.fail("ingest_url must not consult saved login")

    monkeypatch.setattr(run_module, "authenticated_feed", unexpected_login)
    return init


def test_ingest_url_environment_defaults(config_only_init, monkeypatch):
    monkeypatch.setenv("FEED_INGEST_URL", "https://ingest.test/v1/training")
    monkeypatch.setenv("FEED_API_KEY", "secret")
    monkeypatch.setenv("FEED", "unrelated/feed")
    monkeypatch.setenv("FEED_URL", "https://unrelated.test")

    config = config_only_init()

    assert config.server_url == "https://ingest.test"
    assert config.endpoint_id == config.feed_reference == "training"
    assert config.client_secret == "secret"
    assert config.bearer_token_provider is None


@pytest.mark.parametrize("api_key", [None, ""])
def test_ingest_url_requires_api_key(config_only_init, api_key):
    with pytest.raises(ValueError, match="api_key"):
        config_only_init(
            ingest_url="https://ingest.test/v1/training/telemetry", api_key=api_key
        )


@pytest.mark.parametrize(
    "extra", [{"feed": "other"}, {"server_url": "https://other.test"}]
)
def test_ingest_url_rejects_conflicting_destinations(config_only_init, extra):
    with pytest.raises(ValueError, match="cannot be combined"):
        config_only_init(
            ingest_url="https://ingest.test/v1/training/telemetry",
            api_key="secret",
            **extra,
        )


@pytest.mark.parametrize(
    "url",
    [
        "https://ingest.test/training",
        "https://ingest.test/v1//telemetry",
        "ftp://ingest.test/v1/training/telemetry",
        "https:///v1/training/telemetry",
        "https://ingest.test:invalid/v1/training/telemetry",
        "https://user:password@ingest.test/v1/training/telemetry",
        "https://ingest.test/v1/training/telemetry?key=secret",
        "https://ingest.test/v1/training/telemetry#fragment",
    ],
)
def test_ingest_url_rejects_invalid_endpoints(config_only_init, url):
    with pytest.raises(ValueError, match="ingest_url"):
        config_only_init(ingest_url=url, api_key="secret")

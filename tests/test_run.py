import feed.run as run_module
from feed import init


def test_disabled_run_needs_no_credentials_or_endpoint(monkeypatch):
    monkeypatch.delenv("FEED_API_KEY", raising=False)
    monkeypatch.delenv("FEED_URL", raising=False)

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

    monkeypatch.delenv("FEED", raising=False)
    monkeypatch.setattr(run_module, "authenticated_feed", authenticate)
    monkeypatch.setattr(run_module, "Client", _Client)
    monkeypatch.setattr(
        run_module.Run, "_emit_run_metadata", lambda *args, **kwargs: True
    )

    run = init()

    assert captured["feed"] is None
    assert captured["config"].endpoint_id == "paper"
    assert run.feed == "Research/paper"

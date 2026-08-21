import feed.run as run_module
from feed import init


def test_disabled_run_needs_no_credentials_or_endpoint(monkeypatch):
    monkeypatch.delenv("FEED_API_KEY", raising=False)
    monkeypatch.delenv("FEED_URL", raising=False)

    def fail_authentication(*_args, **_kwargs):
        raise AssertionError("disabled runs must not authenticate")

    monkeypatch.setattr(run_module, "authenticated_project", fail_authentication)

    run = init(project="lab/project", enabled=False)

    assert not run.log("train", {"unsupported": object(), "missing": None})
    assert not run.log_wait("train", {"values": []}, timeout=0)
    assert run.flush().successful
    assert run.finish().successful


def test_disabled_run_still_has_an_identity():
    run = init(project="lab/project", enabled=False)
    assert run.project == "lab/project"
    assert run.id

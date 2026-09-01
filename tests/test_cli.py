import builtins

from feed import cli
from feed.credentials import CredentialStore


class _Response:
    ok = True
    status_code = 200
    reason = "OK"

    def json(self):
        return {
            "id": "attachment-1",
            "name": "Feed project data",
            "kind": "connection",
            "created_at": "2026-08-22T00:00:00.000Z",
            "secret_config_set": True,
        }


class _AuthResponse(_Response):
    def json(self):
        return {
            "access_token": "access-1",
            "refresh_token": "refresh-1",
        }


def test_login_accepts_positional_deployment_url(monkeypatch):
    captured = {}

    def login(args):
        captured["deployment_url"] = args.deployment_url
        captured["server_url"] = args.server_url
        return 0

    monkeypatch.setattr(cli, "_login", login)

    assert cli.main(["login", "https://feed.test/ingest"]) == 0
    assert captured == {
        "deployment_url": "https://feed.test/ingest",
        "server_url": None,
    }


def test_login_selects_the_only_feed_by_default(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    project = {
        "id": "018f47a8-a82b-7f10-8000-000000000001",
        "organization_slug": "lab",
        "name": "Paper",
        "role": "owner",
    }
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)
    monkeypatch.setattr(cli.requests, "post", lambda *args, **kwargs: _AuthResponse())
    monkeypatch.setattr(cli, "fetch_projects", lambda *_args: [project])
    monkeypatch.setattr(
        cli,
        "fetch_feeds",
        lambda *_args: [
            {
                "id": "018f47a8-a82b-7f10-8000-000000000101",
                "project_reference": "Paper",
                "slug": "training",
                "phase": "ready",
                "role": "owner",
                "lifecycle": "active",
                "ingest_url": "https://training.feed.test/v1/training",
            }
        ],
    )
    monkeypatch.setattr(builtins, "input", lambda _prompt: "one-time-code")

    assert (
        cli.main(
            [
                "login",
                "https://feed.test/ingest",
                "--control-url",
                "https://control.test",
                "--provider",
                "github",
            ]
        )
        == 0
    )

    assert store.load()["default_feed"].endswith("0101")
    output = capsys.readouterr().out
    assert "Paper/training\tready\towner\tdefault" in output
    assert "Using the only available feed by default: Paper/training" in output


def test_use_selects_a_cached_feed(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(
        {
            "version": 1,
            "server_url": "https://feed.test/ingest",
            "refresh_token": "refresh-1",
            "feeds": [
                {
                    "id": "018f47a8-a82b-7f10-8000-000000000101",
                    "project_reference": "Paper",
                    "slug": "training",
                    "role": "owner",
                    "phase": "ready",
                },
                {
                    "id": "018f47a8-a82b-7f10-8000-000000000102",
                    "project_reference": "Paper",
                    "slug": "evaluation",
                    "role": "editor",
                    "phase": "ready",
                },
            ],
        }
    )
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)

    assert cli.main(["use", "Paper/training"]) == 0

    assert store.load()["default_feed"].endswith("0101")
    assert "Default feed: Paper/training" in capsys.readouterr().out


def test_list_refreshes_project_and_feed_catalog(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(
        {
            "version": 1,
            "control_url": "https://control.test",
            "server_url": "https://feed.test/ingest",
            "refresh_token": "refresh-1",
            "projects": [],
            "feeds": [],
        }
    )
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)

    class _Tokens:
        def __init__(self, _store):
            pass

        def token(self):
            return "control-access"

    monkeypatch.setattr(cli, "TokenProvider", _Tokens)
    project = {
        "id": "018f47a8-a82b-7f10-8000-000000000001",
        "name": "Paper",
        "role": "owner",
    }
    feed = {
        "id": "018f47a8-a82b-7f10-8000-000000000101",
        "project_reference": "Paper",
        "slug": "training",
        "phase": "provisioning",
        "role": "owner",
    }
    monkeypatch.setattr(cli, "fetch_projects", lambda *_args: [project])
    monkeypatch.setattr(cli, "fetch_feeds", lambda *_args: [feed])

    assert cli.main(["list"]) == 0
    assert store.load()["feeds"] == [feed]
    assert "Paper/training\tprovisioning\towner" in capsys.readouterr().out

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


def test_login_selects_the_only_project_by_default(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    project = {
        "id": "018f47a8-a82b-7f10-8000-000000000001",
        "organization_slug": "lab",
        "slug": "paper",
        "name": "Paper",
        "role": "owner",
    }
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)
    monkeypatch.setattr(cli.requests, "post", lambda *args, **kwargs: _AuthResponse())
    monkeypatch.setattr(cli, "fetch_projects", lambda *_args: [project])
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

    assert store.load()["default_project"] == project["id"]
    output = capsys.readouterr().out
    assert "lab/paper\tPaper\towner\tdefault" in output
    assert "Using the only available project by default: lab/paper" in output


def test_use_selects_a_cached_project(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(
        {
            "version": 1,
            "server_url": "https://feed.test/ingest",
            "refresh_token": "refresh-1",
            "projects": [
                {
                    "id": "018f47a8-a82b-7f10-8000-000000000001",
                    "organization_slug": "lab",
                    "slug": "paper",
                    "role": "owner",
                },
                {
                    "id": "018f47a8-a82b-7f10-8000-000000000002",
                    "organization_slug": "lab",
                    "slug": "other",
                    "role": "editor",
                },
            ],
        }
    )
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)

    assert cli.main(["use", "lab/paper"]) == 0

    assert store.load()["default_project"].endswith("0001")
    assert "Default Feed project: lab/paper" in capsys.readouterr().out


def test_enable_resolves_project_and_uses_control_token(tmp_path, monkeypatch, capsys):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(
        {
            "version": 1,
            "control_url": "https://control.test",
            "server_url": "https://feed.test/ingest",
            "refresh_token": "refresh-1",
            "projects": [
                {
                    "id": "018f47a8-a82b-7f10-8000-000000000001",
                    "organization_slug": "lab",
                    "slug": "paper",
                    "role": "owner",
                }
            ],
        }
    )
    monkeypatch.setattr(cli, "CredentialStore", lambda: store)

    class _Tokens:
        def __init__(self, _store):
            pass

        def token(self):
            return "control-access"

    monkeypatch.setattr(cli, "TokenProvider", _Tokens)
    calls = []

    def post(url, *, headers, timeout):
        calls.append((url, headers, timeout))
        return _Response()

    monkeypatch.setattr(cli.requests, "post", post)

    assert cli.main(["enable", "lab/paper"]) == 0
    assert calls[0][0].endswith(
        "/v1/projects/018f47a8-a82b-7f10-8000-000000000001/feed-local/enable"
    )
    assert calls[0][1]["Authorization"] == "Bearer control-access"
    assert calls[0][1]["Idempotency-Key"]
    assert "Feed project data is enabled for lab/paper." in capsys.readouterr().out

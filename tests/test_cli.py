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

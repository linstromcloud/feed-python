import base64
import json
import os
import stat
import threading
import time

from feed.credentials import (
    CredentialStore,
    TokenProvider,
    authenticated_feed,
    credential_control_url,
    feed_reference,
    fetch_feeds,
    fetch_projects,
)


def _jwt(exp):
    payload = (
        base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode())
        .rstrip(b"=")
        .decode()
    )
    return f"header.{payload}.signature"


class _Response:
    ok = True
    status_code = 200
    reason = "OK"

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def _credentials(refresh="refresh-1"):
    return {
        "version": 1,
        "control_url": "https://control.test",
        "server_url": "https://feed.test/ingest",
        "refresh_token": refresh,
        "projects": [],
    }


def test_rotated_refresh_is_atomic_private_and_cached(tmp_path, monkeypatch):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(_credentials())
    calls = []

    def post(url, *, json, headers, timeout):
        calls.append((url, json, headers, timeout))
        return _Response(
            {
                "access_token": _jwt(time.time() + 600),
                "refresh_token": "refresh-2",
                "expires_in": 600,
            }
        )

    monkeypatch.setattr("feed.credentials.requests.post", post)
    provider = TokenProvider(store)
    tokens = []
    threads = [
        threading.Thread(target=lambda: tokens.append(provider())) for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(tokens) == 4
    assert len(calls) == 1
    assert calls[0][1]["refresh_token"] == "refresh-1"
    assert calls[0][1]["resource"] == "feed"
    assert store.load()["refresh_token"] == "refresh-2"
    if os.name != "nt":  # Windows access is controlled by inherited ACLs.
        assert stat.S_IMODE(os.stat(store.path).st_mode) == 0o600


def test_control_token_uses_default_auth_resource(tmp_path, monkeypatch):
    store = CredentialStore(tmp_path / "credentials.json")
    store.save(_credentials())
    calls = []

    def post(url, *, json, headers, timeout):
        calls.append((url, json, headers, timeout))
        return _Response(
            {
                "access_token": _jwt(time.time() + 600),
                "refresh_token": "refresh-2",
            }
        )

    monkeypatch.setattr("feed.credentials.requests.post", post)

    TokenProvider(store).token()

    assert calls[0][1] == {"refresh_token": "refresh-1"}


def test_control_url_defaults_to_ingest_origin():
    assert (
        credential_control_url({"server_url": "https://feed.test/ingest"})
        == "https://feed.test"
    )


def test_fetch_projects_uses_slugless_me_listing(monkeypatch):
    project_id = "018f47a8-a82b-7f10-8000-000000000001"
    calls = []

    def get(url, *, headers, timeout):
        calls.append((url, headers, timeout))
        assert url == "https://control.test/v1/me/projects"
        return _Response(
            {
                "items": [
                    {
                        "project_id": project_id,
                        "name": "GAP",
                        "owning_organization": {
                            "id": "018f47a8-a82b-7f10-8000-000000000010",
                            "slug": "gapindnns",
                            "display_name": "GAP",
                        },
                        "role": "owner",
                    }
                ]
            }
        )

    monkeypatch.setattr("feed.credentials.requests.get", get)

    assert fetch_projects("https://control.test", "access-1") == [
        {
            "id": project_id,
            "name": "GAP",
            "organization_slug": "gapindnns",
            "role": "owner",
        }
    ]
    assert len(calls) == 1


def test_fetch_feeds_builds_copyable_project_feed_references(monkeypatch):
    project = {
        "id": "018f47a8-a82b-7f10-8000-000000000001",
        "organization_slug": "lab",
        "name": "Research",
        "role": "owner",
    }

    def get(url, *, headers, timeout):
        assert url.endswith(f"/v1/projects/{project['id']}/feeds")
        return _Response(
            {
                "items": [
                    {
                        "id": "018f47a8-a82b-7f10-8000-000000000101",
                        "name": "Main feed",
                        "slug": "paper",
                        "kind": "inhouse",
                        "lifecycle": "active",
                        "status": {
                            "phase": "ready",
                            "ingest_url": "https://paper.feed.test/v1/paper",
                        },
                    }
                ]
            }
        )

    monkeypatch.setattr("feed.credentials.requests.get", get)
    feeds = fetch_feeds("https://control.test", "access-1", [project])

    assert feed_reference(feeds[0], feeds) == "Research/paper"
    assert feeds[0]["ingest_url"] == "https://paper.feed.test/v1/paper"


def test_authenticated_feed_uses_sole_cached_feed(tmp_path):
    store = CredentialStore(tmp_path / "credentials.json")
    feed = {
        "id": "018f47a8-a82b-7f10-8000-000000000101",
        "project_reference": "Research",
        "slug": "paper",
        "lifecycle": "active",
        "phase": "ready",
        "ingest_url": "https://paper.feed.test/v1/paper",
    }
    credentials = _credentials()
    credentials["feeds"] = [feed]
    store.save(credentials)

    url, slug, _provider, reference = authenticated_feed(None, store=store)

    assert url == "https://paper.feed.test"
    assert slug == "paper"
    assert reference == "Research/paper"


def test_concurrent_processes_serialize_credential_updates(tmp_path):
    import subprocess
    import sys

    store = CredentialStore(tmp_path / "credentials.json")
    store.save(_credentials("0"))
    script = """
import sys, time
from pathlib import Path
from feed.credentials import CredentialStore
store = CredentialStore(Path(sys.argv[1]))
print("ready", flush=True)
sys.stdin.readline()
def advance(credentials):
    previous = int(credentials["refresh_token"])
    time.sleep(0.01)
    credentials["refresh_token"] = str(previous + 1)
for _ in range(10):
    store.update(advance)
"""
    children = [
        subprocess.Popen(
            [sys.executable, "-c", script, str(store.path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(4)
    ]
    try:
        for child in children:
            assert child.stdout.readline().strip() == "ready"
        for child in children:
            child.stdin.write("go\n")
            child.stdin.flush()
        for child in children:
            _, error = child.communicate(timeout=15)
            assert child.returncode == 0, error
        assert store.load()["refresh_token"] == "40"
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()

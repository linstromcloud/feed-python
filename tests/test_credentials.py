import base64
import json
import os
import stat
import threading
import time

import pytest

from feed.credentials import (
    CredentialStore,
    TokenProvider,
    credential_control_url,
    resolve_project,
)
from feed.errors import AuthError


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


def test_project_reference_supports_canonical_plain_and_uuid():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "organization_slug": "lab-a",
            "slug": "paper",
        },
        {
            "id": "018f47a8-a82b-7f10-8000-000000000002",
            "organization_slug": "lab-b",
            "slug": "shared",
        },
        {
            "id": "018f47a8-a82b-7f10-8000-000000000003",
            "organization_slug": "lab-c",
            "slug": "shared",
        },
    ]
    assert resolve_project("paper", projects).endswith("0001")
    assert resolve_project("lab-b/shared", projects).endswith("0002")
    assert resolve_project("018f47a8-a82b-7f10-8000-000000000003", projects).endswith(
        "0003"
    )
    with pytest.raises(AuthError, match="ambiguous"):
        resolve_project("shared", projects)

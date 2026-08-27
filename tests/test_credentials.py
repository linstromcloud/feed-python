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
    authenticated_project,
    credential_control_url,
    fetch_projects,
    resolve_project,
    select_project,
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


def test_slugless_project_resolves_by_unique_name():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "name": "GAP",
            "organization_slug": "gapindnns",
        }
    ]

    project_id, reference = select_project("gap", projects)

    assert project_id == projects[0]["id"]
    assert reference == "GAP"


def test_duplicate_project_names_require_uuid():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "name": "GAP",
            "organization_slug": "lab-a",
        },
        {
            "id": "018f47a8-a82b-7f10-8000-000000000002",
            "name": "GAP",
            "organization_slug": "lab-b",
        },
    ]

    with pytest.raises(AuthError, match="ambiguous") as error:
        resolve_project("GAP", projects)

    assert projects[0]["id"] in str(error.value)
    assert projects[1]["id"] in str(error.value)
    assert select_project(None, projects, projects[0]["id"])[1] == projects[0]["id"]


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


def test_project_selection_prefers_saved_default_and_returns_reference():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "organization_slug": "lab",
            "slug": "paper",
        },
        {
            "id": "018f47a8-a82b-7f10-8000-000000000002",
            "organization_slug": "lab",
            "slug": "other",
        },
    ]

    project_id, reference = select_project(None, projects, projects[1]["id"])

    assert project_id == projects[1]["id"]
    assert reference == "lab/other"


def test_project_selection_uses_the_only_available_project_without_default():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "organization_slug": "lab",
            "slug": "paper",
        }
    ]

    project_id, reference = select_project(None, projects)

    assert project_id == projects[0]["id"]
    assert reference == "lab/paper"


def test_project_selection_fails_loudly_when_ambiguous():
    projects = [
        {
            "id": "018f47a8-a82b-7f10-8000-000000000001",
            "organization_slug": "lab",
            "slug": "paper",
        },
        {
            "id": "018f47a8-a82b-7f10-8000-000000000002",
            "organization_slug": "lab",
            "slug": "other",
        },
    ]

    with pytest.raises(AuthError, match="Feed project is ambiguous") as error:
        select_project(None, projects)

    assert "feed use PROJECT_NAME_OR_ID" in str(error.value)
    assert "lab/other" in str(error.value)
    assert "lab/paper" in str(error.value)


def test_authenticated_project_uses_sole_cached_project(tmp_path):
    store = CredentialStore(tmp_path / "credentials.json")
    project = {
        "id": "018f47a8-a82b-7f10-8000-000000000001",
        "organization_slug": "lab",
        "slug": "paper",
    }
    credentials = _credentials()
    credentials["projects"] = [project]
    store.save(credentials)

    url, project_id, _provider, reference = authenticated_project(None, store=store)

    assert url == "https://feed.test/ingest"
    assert project_id == project["id"]
    assert reference == "lab/paper"

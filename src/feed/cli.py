"""Terminal login and project discovery for Feed."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import secrets
import sys
import urllib.parse
import uuid
from typing import Any, Dict, List, Optional

import requests

from .credentials import (
    CredentialStore,
    TokenProvider,
    _json_response,
    credential_control_url,
    fetch_projects,
    project_reference,
    resolve_project,
)
from .errors import AuthError

_CLIENT_ID = "feed-cli"
_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="feed")
    subcommands = parser.add_subparsers(dest="command", required=True)

    login = subcommands.add_parser(
        "login", help="sign in and cache your logging projects"
    )
    login.add_argument("deployment_url", nargs="?", metavar="URL")
    login.add_argument("--server-url")
    login.add_argument("--control-url", default=os.environ.get("FEED_CONTROL_URL"))
    login.add_argument("--provider", choices=("github", "entra", "magic_link"))
    login.add_argument("--organization")

    projects = subcommands.add_parser(
        "projects", help="refresh and list logging projects"
    )
    projects.add_argument("--no-refresh", action="store_true")

    use = subcommands.add_parser(
        "use", help="select the default project for logging"
    )
    use.add_argument("project", help="unique project name or UUID")

    enable = subcommands.add_parser(
        "enable", help="enable the managed project catalog for querying"
    )
    enable.add_argument("project", help="unique project name or UUID")

    args = parser.parse_args(argv)
    try:
        if args.command == "login":
            return _login(args)
        if args.command == "projects":
            return _projects(not args.no_refresh)
        if args.command == "use":
            return _use(args.project)
        return _enable(args.project)
    except AuthError as exc:
        parser.exit(1, f"feed: {exc}\n")


def _login(args: argparse.Namespace) -> int:
    if args.deployment_url and args.server_url:
        raise AuthError(
            "provide the deployment URL either positionally or with --server-url"
        )
    server_url = args.server_url or args.deployment_url or os.environ.get("FEED_URL")
    if not server_url:
        raise AuthError(
            "deployment URL is required; run `feed login https://.../ingest`"
        )
    server_origin = _origin(server_url)
    control_url = args.control_url or server_origin
    provider = args.provider or _select_provider(control_url)

    verifier = _b64url(secrets.token_bytes(32))
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    device_guid = str(uuid.uuid4())
    query = {
        "response_type": "code",
        "client_id": _CLIENT_ID,
        "redirect_uri": _REDIRECT_URI,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": _b64url(secrets.token_bytes(16)),
        "device_guid": device_guid,
        "provider": provider,
    }
    if args.organization:
        query["organization"] = args.organization
    authorize_url = (
        f"{control_url.rstrip('/')}/v1/auth/authorize?{urllib.parse.urlencode(query)}"
    )
    print("Open this URL in a browser and complete sign-in:\n")
    print(authorize_url)
    print()
    code = input("Paste the one-time code: ").strip()
    if not code:
        raise AuthError("no authorization code entered")

    response = requests.post(
        f"{control_url.rstrip('/')}/v1/auth/oauth/token",
        data={
            "grant_type": "authorization_code",
            "client_id": _CLIENT_ID,
            "redirect_uri": _REDIRECT_URI,
            "code": code,
            "code_verifier": verifier,
        },
        headers={"Idempotency-Key": str(uuid.uuid4())},
        timeout=15,
    )
    body = _json_response(response, "exchange authorization code")
    refresh_token = body.get("refresh_token")
    access_token = body.get("access_token")
    if not isinstance(refresh_token, str) or not isinstance(access_token, str):
        raise AuthError("authorization service returned an incomplete login response")
    project_records = fetch_projects(control_url, access_token)

    default_project = (
        str(project_records[0]["id"])
        if len(project_records) == 1 and project_records[0].get("id")
        else None
    )
    credentials = {
        "version": 1,
        "control_url": control_url.rstrip("/"),
        "server_url": server_url.rstrip("/"),
        "device_guid": device_guid,
        "refresh_token": refresh_token,
        "projects": project_records,
    }
    if default_project is not None:
        credentials["default_project"] = default_project
    CredentialStore().save(credentials)
    print(f"Signed in. {len(project_records)} project(s) are available for logging.")
    _print_projects(project_records, default_project)
    if default_project is not None:
        print(
            "Using the only available project by default: "
            f"{project_reference(default_project, project_records)}"
        )
    return 0


def _projects(refresh: bool) -> int:
    store = CredentialStore()
    credentials = store.load()
    records = credentials.get("projects", [])
    if refresh:
        provider = TokenProvider(store)
        records = fetch_projects(credential_control_url(credentials), provider.token())
        store.update(lambda current: current.__setitem__("projects", records))
    _print_projects(records, credentials.get("default_project"))
    return 0


def _use(project: str) -> int:
    store = CredentialStore()
    credentials = store.load()
    records = credentials.get("projects", [])
    if not isinstance(records, list):
        records = []

    try:
        project_id = resolve_project(project, records)
    except AuthError as error:
        if "ambiguous" in str(error):
            raise
        project_id = ""

    if not any(
        isinstance(record, dict) and record.get("id") == project_id
        for record in records
    ):
        provider = TokenProvider(store)
        records = fetch_projects(credential_control_url(credentials), provider.token())
        project_id = resolve_project(project, records)
        if not any(record.get("id") == project_id for record in records):
            raise AuthError(
                f"project {project!r} is not available for logging; run `feed projects`"
            )

    def select(current: Dict[str, Any]) -> None:
        current["projects"] = records
        current["default_project"] = project_id

    store.update(select)
    reference = project_reference(project_id, records)
    print(f"Default Feed project: {reference}")
    return 0


def _enable(project: str) -> int:
    store = CredentialStore()
    credentials = store.load()
    project_id = resolve_project(project, credentials.get("projects", []))
    access_token = TokenProvider(store).token()
    response = requests.post(
        f"{credential_control_url(credentials)}/v1/projects/{project_id}/feed-local/enable",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Idempotency-Key": str(uuid.uuid4()),
        },
        timeout=15,
    )
    _json_response(response, f"enable Feed project {project}")
    print(f"Feed project data is enabled for {project}.")
    return 0


def _print_projects(records: Any, default_project: Optional[str] = None) -> None:
    if not records:
        print("No projects with logging permission.")
        return
    for project in records:
        reference = project_reference(str(project["id"]), records)
        organization = project.get("organization_slug") or "-"
        selected = "\tdefault" if project.get("id") == default_project else ""
        print(f"{reference}\t{organization}\t{project['role']}{selected}")


def _select_provider(control_url: str) -> str:
    response = requests.get(f"{control_url.rstrip('/')}/v1/auth/providers", timeout=15)
    body = _json_response(response, "discover login providers")
    providers: List[Dict[str, Any]] = body.get("providers", [])
    kinds = [item.get("kind") for item in providers if item.get("kind")]
    if len(kinds) == 1:
        return str(kinds[0])
    if not kinds:
        raise AuthError("authorization service has no configured login providers")
    if not sys.stdin.isatty():
        raise AuthError(f"choose --provider from: {', '.join(kinds)}")
    for index, item in enumerate(providers, 1):
        print(f"{index}. {item.get('label') or item['kind']} ({item['kind']})")
    choice = input("Login provider: ").strip()
    try:
        return str(kinds[int(choice) - 1])
    except (ValueError, IndexError):
        raise AuthError("invalid login provider selection") from None


def _origin(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise AuthError(f"invalid server URL: {url!r}")
    return f"{parsed.scheme}://{parsed.netloc}"


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


if __name__ == "__main__":
    raise SystemExit(main())

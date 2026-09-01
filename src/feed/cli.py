"""Terminal login and feed discovery."""

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
    feed_reference,
    fetch_feeds,
    fetch_projects,
    resolve_feed,
)
from .errors import AuthError

_CLIENT_ID = "feed-cli"
_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="feed")
    subcommands = parser.add_subparsers(dest="command", required=True)

    login = subcommands.add_parser(
        "login", help="sign in and cache feeds available for logging"
    )
    login.add_argument("deployment_url", nargs="?", metavar="URL")
    login.add_argument("--server-url")
    login.add_argument("--control-url", default=os.environ.get("FEED_CONTROL_URL"))
    login.add_argument("--provider", choices=("github", "entra", "magic_link"))
    login.add_argument("--organization")

    list_command = subcommands.add_parser(
        "list", help="refresh and list available project/feed references"
    )
    list_command.add_argument("--no-refresh", action="store_true")

    use = subcommands.add_parser("use", help="select the default feed for logging")
    use.add_argument("feed", help="project/feed reference printed by `feed list`")

    args = parser.parse_args(argv)
    try:
        if args.command == "login":
            return _login(args)
        if args.command == "list":
            return _list(not args.no_refresh)
        if args.command == "use":
            return _use(args.feed)
        raise AssertionError(f"unhandled command: {args.command}")
    except AuthError as exc:
        parser.exit(1, f"feed: {exc}\n")


def _login(args: argparse.Namespace) -> int:
    if args.deployment_url and args.server_url:
        raise AuthError(
            "provide the deployment URL either positionally or with --server-url"
        )
    server_url = args.server_url or args.deployment_url or os.environ.get("FEED_URL")
    if not server_url:
        raise AuthError("deployment URL is required; run `feed login https://...`")
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
    feed_records = fetch_feeds(control_url, access_token, project_records)

    default_feed = (
        str(feed_records[0]["id"])
        if len(feed_records) == 1 and feed_records[0].get("id")
        else None
    )
    credentials = {
        "version": 1,
        "control_url": control_url.rstrip("/"),
        "server_url": server_url.rstrip("/"),
        "device_guid": device_guid,
        "refresh_token": refresh_token,
        "projects": project_records,
        "feeds": feed_records,
    }
    if default_feed is not None:
        credentials["default_feed"] = default_feed
    CredentialStore().save(credentials)
    print(f"Signed in. {len(feed_records)} feed(s) are available for logging.")
    _print_feeds(feed_records, default_feed)
    if default_feed is not None:
        print(
            "Using the only available feed by default: "
            f"{feed_reference(feed_records[0], feed_records)}"
        )
    return 0


def _list(refresh: bool) -> int:
    store = CredentialStore()
    credentials = store.load()
    records = credentials.get("feeds", [])
    if refresh:
        provider = TokenProvider(store)
        control_url = credential_control_url(credentials)
        projects = fetch_projects(control_url, provider.token())
        records = fetch_feeds(control_url, provider.token(), projects)

        def update(current: Dict[str, Any]) -> None:
            current["projects"] = projects
            current["feeds"] = records

        store.update(update)
    _print_feeds(records, credentials.get("default_feed"))
    return 0


def _use(feed: str) -> int:
    store = CredentialStore()
    credentials = store.load()
    records = credentials.get("feeds", [])
    refreshed_projects = None
    if not isinstance(records, list):
        records = []

    try:
        selected = resolve_feed(feed, records)
    except AuthError as error:
        if "ambiguous" in str(error):
            raise
        selected = {}

    if not any(
        isinstance(record, dict) and record.get("id") == selected.get("id")
        for record in records
    ):
        provider = TokenProvider(store)
        control_url = credential_control_url(credentials)
        refreshed_projects = fetch_projects(control_url, provider.token())
        records = fetch_feeds(control_url, provider.token(), refreshed_projects)
        selected = resolve_feed(feed, records)

    def select(current: Dict[str, Any]) -> None:
        if refreshed_projects is not None:
            current["projects"] = refreshed_projects
        current["feeds"] = records
        current["default_feed"] = selected["id"]

    store.update(select)
    print(f"Default feed: {feed_reference(selected, records)}")
    return 0


def _print_feeds(records: Any, default_feed: Optional[str] = None) -> None:
    if not records:
        print("No feeds are available for logging.")
        return
    for feed in records:
        reference = feed_reference(feed, records)
        selected = "\tdefault" if feed.get("id") == default_feed else ""
        print(
            f"{reference}\t{feed.get('phase', 'pending')}\t"
            f"{feed.get('role', '-')}{selected}"
        )


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

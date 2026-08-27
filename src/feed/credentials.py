"""User credentials and project resolution for a Feed deployment.

The refresh token is a rotating credential. Every read/rotate/write cycle holds
an advisory lock beside the credential file, so concurrent processes sharing a
home directory always start from the latest successor token.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
import urllib.parse
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import requests

from .errors import AuthError

try:
    import fcntl
except ImportError:  # pragma: no cover - process locking requires POSIX
    fcntl = None  # type: ignore[assignment]

_VERSION = 1
_ACCESS_TOKEN_MARGIN_SECONDS = 60
_CREDENTIAL_KEYS = {
    "version",
    "control_url",
    "server_url",
    "device_guid",
    "refresh_token",
    "projects",
    "default_project",
}


def default_credentials_path() -> Path:
    override = os.environ.get("FEED_CREDENTIALS_FILE")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".config" / "feed" / "credentials.json"


class CredentialStore:
    """Atomic, process-safe access to one rotating Feed login."""

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = path or default_credentials_path()
        self.lock_path = self.path.with_name(self.path.name + ".lock")

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        lock_fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            os.chmod(self.lock_path, 0o600)
            if fcntl is None:
                raise AuthError(
                    "shared credential locking requires a POSIX platform with fcntl"
                )
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)

    def load(self) -> Dict[str, Any]:
        with self.locked():
            return self._read_unlocked()

    def save(self, credentials: Dict[str, Any]) -> None:
        with self.locked():
            self._write_unlocked(credentials)

    def update(self, mutate: Callable[[Dict[str, Any]], None]) -> Dict[str, Any]:
        with self.locked():
            credentials = self._read_unlocked()
            mutate(credentials)
            self._write_unlocked(credentials)
            return credentials

    def _read_unlocked(self) -> Dict[str, Any]:
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                credentials = json.load(handle)
        except FileNotFoundError as exc:
            raise AuthError(
                "not logged in; run `feed login https://.../ingest` first"
            ) from exc
        except (OSError, ValueError) as exc:
            raise AuthError(f"cannot read Feed credentials: {exc}") from exc
        if credentials.get("version") != _VERSION or not credentials.get(
            "refresh_token"
        ):
            raise AuthError("Feed credential file is invalid or unsupported")
        return credentials

    def _write_unlocked(self, credentials: Dict[str, Any]) -> None:
        credentials = {
            key: value for key, value in credentials.items() if key in _CREDENTIAL_KEYS
        }
        credentials["version"] = _VERSION
        tmp = self.path.with_name(f".{self.path.name}.{uuid.uuid4().hex}.tmp")
        try:
            fd = os.open(tmp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(credentials, handle, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, self.path)
            os.chmod(self.path, 0o600)
            directory_fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


class TokenProvider:
    """Mint and cache audience-specific access tokens from one refresh family."""

    def __init__(self, store: Optional[CredentialStore] = None) -> None:
        self.store = store or CredentialStore()
        self._mutex = threading.Lock()
        self._cache: Dict[str, Tuple[str, float]] = {}

    def __call__(self) -> str:
        return self.token("feed")

    def token(self, resource: Optional[str] = None) -> str:
        cache_key = resource or ""
        with self._mutex:
            cached = self._cache.get(cache_key)
            if cached and time.time() + _ACCESS_TOKEN_MARGIN_SECONDS < cached[1]:
                return cached[0]

            # The file lock spans the network rotation and atomic successor
            # write. Other jobs wait rather than presenting the spent token.
            with self.store.locked():
                credentials = self.store._read_unlocked()
                control_url = credential_control_url(credentials)
                payload = {"refresh_token": credentials["refresh_token"]}
                if resource:
                    payload["resource"] = resource
                response = requests.post(
                    f"{control_url}/v1/auth/token/refresh",
                    json=payload,
                    headers={"Idempotency-Key": str(uuid.uuid4())},
                    timeout=15,
                )
                body = _json_response(response, "refresh Feed login")
                access_token = body.get("access_token")
                successor = body.get("refresh_token")
                if not isinstance(access_token, str) or not isinstance(successor, str):
                    raise AuthError(
                        "authorization service returned an incomplete refresh response"
                    )
                credentials["control_url"] = control_url
                credentials["refresh_token"] = successor
                self.store._write_unlocked(credentials)

            expires_at = _jwt_exp(access_token)
            if expires_at is None:
                expires_at = time.time() + max(0, int(body.get("expires_in", 0)))
            self._cache[cache_key] = (access_token, expires_at)
            return access_token


def credential_control_url(credentials: Dict[str, Any]) -> str:
    """Return the control API origin stored with, or implied by, a login."""

    explicit = str(credentials.get("control_url", "")).rstrip("/")
    if explicit:
        return explicit
    server_url = str(credentials.get("server_url", ""))
    parsed = urllib.parse.urlsplit(server_url)
    if parsed.scheme in ("http", "https") and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    raise AuthError("Feed credentials contain no control API URL")


def select_project(
    reference: Optional[str],
    projects: Any,
    default_project: Optional[str] = None,
) -> Tuple[str, str]:
    """Select a project and return its UUID and user-facing reference."""

    requested = str(reference or "").strip() or str(default_project or "").strip()
    if requested:
        project_id = resolve_project(requested, projects)
        return project_id, project_reference(project_id, projects)

    records = projects if isinstance(projects, list) else []
    available = [
        project
        for project in records
        if isinstance(project, dict) and isinstance(project.get("id"), str)
    ]
    if len(available) == 1:
        project_id = str(available[0]["id"])
        return project_id, project_reference(project_id, available)
    if not available:
        raise AuthError(
            "no Feed projects are available for logging; run `feed projects`"
        )

    choices = ", ".join(
        sorted(
            project_reference(str(project["id"]), available) for project in available
        )
    )
    raise AuthError(
        "Feed project is ambiguous: no default is selected and "
        f"{len(available)} projects are available ({choices}). "
        "Run `feed use PROJECT_NAME_OR_ID` or pass project= explicitly."
    )


def project_reference(project_id: str, projects: Any) -> str:
    """Return a unique project name, legacy slug reference, or UUID."""

    if isinstance(projects, list):
        for project in projects:
            if not isinstance(project, dict) or project.get("id") != project_id:
                continue
            name = str(project.get("name", "")).strip()
            same_name = [
                item
                for item in projects
                if isinstance(item, dict)
                and str(item.get("name", "")).strip().casefold()
                == name.casefold()
            ]
            if name and len(same_name) == 1:
                return name
            # Credentials written by Feed versions predating the slugless
            # Duckvis project contract remain usable until the next refresh.
            organization = str(project.get("organization_slug", "")).strip()
            slug = str(project.get("slug", "")).strip()
            if organization and slug:
                return f"{organization}/{slug}"
            if slug:
                return slug
    return project_id


def authenticated_project(
    project: Optional[str],
    server_url: Optional[str] = None,
    store: Optional[CredentialStore] = None,
) -> Tuple[str, str, TokenProvider, str]:
    """Resolve a user-facing project reference to its canonical UUID."""

    store = store or CredentialStore()
    credentials = store.load()
    resolved_url = server_url or str(credentials.get("server_url", ""))
    if not resolved_url:
        raise AuthError("Feed credentials contain no ingest server URL")
    provider = TokenProvider(store)
    projects = credentials.get("projects", [])
    default_project = credentials.get("default_project")
    try:
        project_id, reference = select_project(project, projects, default_project)
    except AuthError as first_error:
        if "ambiguous" in str(first_error):
            raise
        projects = fetch_projects(credential_control_url(credentials), provider.token())
        store.update(lambda current: current.__setitem__("projects", projects))
        try:
            project_id, reference = select_project(
                project, projects, default_project
            )
        except AuthError as refreshed_error:
            raise refreshed_error from first_error
    return resolved_url.rstrip("/"), project_id, provider, reference


def resolve_project(reference: str, projects: Any) -> str:
    reference = reference.strip()
    try:
        return str(uuid.UUID(reference))
    except ValueError:
        pass
    if not isinstance(projects, list):
        projects = []
    normalized = reference.lower().strip("/")
    by_name = [
        project
        for project in projects
        if str(project.get("name", "")).strip().casefold() == normalized.casefold()
    ]
    if len(by_name) == 1:
        return str(by_name[0]["id"])
    if len(by_name) > 1:
        choices = ", ".join(sorted(str(item["id"]) for item in by_name))
        raise AuthError(
            f"project name {reference!r} is ambiguous; use one of these UUIDs: {choices}"
        )

    # Legacy cached credentials may still carry the retired project slug.
    canonical = [
        project
        for project in projects
        if f"{project.get('organization_slug', '')}/{project.get('slug', '')}".lower()
        == normalized
    ]
    if len(canonical) == 1:
        return str(canonical[0]["id"])
    by_slug = [
        project
        for project in projects
        if str(project.get("slug", "")).lower() == normalized
    ]
    if len(by_slug) == 1:
        return str(by_slug[0]["id"])
    if len(by_slug) > 1:
        choices = ", ".join(
            sorted(f"{item['organization_slug']}/{item['slug']}" for item in by_slug)
        )
        raise AuthError(f"project {reference!r} is ambiguous; use one of: {choices}")
    raise AuthError(
        f"project {reference!r} is not available for logging; run `feed projects`"
    )


def fetch_projects(control_url: str, access_token: str) -> List[Dict[str, Any]]:
    """Return projects on which the current member holds ``Project.log_data``."""

    base = control_url.rstrip("/")
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(f"{base}/v1/me/projects", headers=headers, timeout=15)
    listing = _json_response(response, "list projects")
    projects: List[Dict[str, Any]] = []
    for item in listing.get("items", []):
        if item.get("role") not in ("owner", "editor"):
            continue
        project_id = item.get("project_id")
        if not isinstance(project_id, str):
            continue
        organization = item.get("owning_organization") or {}
        projects.append(
            {
                "id": project_id,
                "name": item.get("name", ""),
                "organization_slug": organization.get("slug", ""),
                "role": item.get("role"),
            }
        )
    return sorted(
        projects,
        key=lambda project: (
            str(project["organization_slug"]).casefold(),
            str(project["name"]).casefold(),
            project["id"],
        ),
    )


def _json_response(response: requests.Response, action: str) -> Dict[str, Any]:
    try:
        body = response.json()
    except ValueError as exc:
        raise AuthError(f"cannot {action}: HTTP {response.status_code}") from exc
    if not response.ok:
        detail = body.get("error_description") or body.get("error") or response.reason
        raise AuthError(f"cannot {action}: {detail}")
    if not isinstance(body, dict):
        raise AuthError(f"cannot {action}: invalid response")
    return body


def _jwt_exp(token: str) -> Optional[float]:
    try:
        segment = token.split(".")[1]
        segment += "=" * (-len(segment) % 4)
        body = json.loads(base64.urlsafe_b64decode(segment.encode("ascii")))
        return float(body["exp"])
    except (IndexError, KeyError, TypeError, ValueError):
        return None

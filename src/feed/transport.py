"""HTTP attempts shared by live workers and explicit spool synchronization."""

import gzip
import logging
import random
import threading
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
import time

import requests

from .blacklist import Blacklist, parse_rules
from .protocol import ProcessedEvent, build_batch_json

logger = logging.getLogger("feed")


@dataclass
class Outcome:
    kind: str
    reason: str = ""
    rules: list = field(default_factory=list)
    retry_after: float = 0
    filtered: set = field(default_factory=set)


def backoff(config, attempt):
    cap = config.retry_max_delay_seconds
    delay = min(config.retry_base_delay_seconds * 2 ** min(attempt, 30), cap)
    return min(cap, delay * random.uniform(0.5, 1.5))


class Transport:
    def __init__(self, config):
        self.config = config
        self.local = threading.local()

    def session(self):
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()
        return self.local.session

    def close(self):
        session = getattr(self.local, "session", None)
        if session is not None:
            session.close()
            del self.local.session

    def headers(self):
        result = {"User-Agent": "feed-python/0.1.0"}
        if self.config.bearer_token_provider:
            result["Authorization"] = f"Bearer {self.config.bearer_token_provider()}"
        elif self.config.client_secret:
            result["X-Client-Secret"] = self.config.client_secret
        return result

    def url(self, route):
        return (
            f"{self.config.server_url.rstrip('/')}/v1/{self.config.endpoint_id}/{route}"
        )

    def blacklist(self):
        try:
            response = self.session().get(
                self.url("blacklist"),
                headers=self.headers(),
                timeout=self.config.blacklist_timeout_seconds,
                allow_redirects=False,
            )
            if response.status_code == 200:
                return Outcome(
                    "success", rules=parse_rules(response.json().get("rules"))
                )
            return self._failure(response)
        except Exception as exc:
            return Outcome("retry", str(exc))

    def upload(self, session_id, events):
        try:
            processed = [
                ProcessedEvent(
                    event["seq"],
                    event["ticket"],
                    event["channel"],
                    event["schema_hash"],
                    event["schema_def"],
                    event["data"],
                )
                for event in events
            ]
            payload = gzip.compress(build_batch_json(session_id, processed))
            headers = self.headers()
            headers.update(
                {"Content-Type": "application/json", "Content-Encoding": "gzip"}
            )
            response = self.session().post(
                self.url("telemetry"),
                data=payload,
                headers=headers,
                timeout=self.config.upload_timeout_seconds,
                allow_redirects=False,
            )
            if 200 <= response.status_code < 300:
                body = response.json()
                if not isinstance(body, dict):
                    return Outcome("retry", "invalid ingestion acknowledgement")
                ingested, dropped = body.get("ingested"), body.get("dropped")
                if (
                    type(ingested) is not int
                    or type(dropped) is not int
                    or min(ingested, dropped) < 0
                    or ingested + dropped != len(events)
                ):
                    return Outcome("retry", "incomplete ingestion acknowledgement")
                rules = parse_rules(body.get("blacklisted"))
                policy = Blacklist()
                policy.set_rules(rules)
                filtered = {
                    event["ticket"]
                    for event in events
                    if policy.is_blacklisted(event["schema_hash"], event["data"])
                }
                matched = len(filtered)
                if matched != dropped:
                    if matched > dropped:
                        filtered.clear()
                    logger.warning(
                        "feed: unexplained server drops=%d (dropped=%d, rule_matches=%d)",
                        dropped - len(filtered),
                        dropped,
                        matched,
                    )
                return Outcome("success", rules=rules, filtered=filtered)
            if response.status_code == 413:
                return Outcome("large", "HTTP 413")
            if (
                response.status_code in (408, 429, 401, 403, 404)
                or 300 <= response.status_code < 400
                or 500 <= response.status_code < 600
            ):
                return self._failure(response)
            return Outcome("failed", f"HTTP {response.status_code}")
        except Exception as exc:
            return Outcome("retry", str(exc))

    def _failure(self, response):
        status = response.status_code
        if status == 401:
            invalidate = getattr(self.config.bearer_token_provider, "invalidate", None)
            if invalidate:
                invalidate()
        delay = 0
        value = response.headers.get("Retry-After")
        if value:
            try:
                delay = max(0, float(value))
            except ValueError:
                try:
                    delay = max(
                        0, parsedate_to_datetime(value).timestamp() - time.time()
                    )
                except (ValueError, TypeError, OverflowError):
                    pass
        if status in (401, 403, 404):
            delay = max(delay, self.config.retry_max_delay_seconds)
        return Outcome("retry", f"HTTP {status}", retry_after=delay)

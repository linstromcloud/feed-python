"""Client-side blacklist: rules fetched at session start (and updated from
upload responses) that drop matching events before they are sent.

A rule matches an event when its ``schema_hash`` equals the event's hash (or is
the wildcard ``"*"``), and — if it has a ``match`` filter — every field in the
filter equals the corresponding field in the event's ``data``. All match values
arrive from the server JSON-stringified, so comparison is done as strings against
the event's stringified field values.
"""

from __future__ import annotations

from typing import Any, Dict, List

WILDCARD = "*"


def _to_match_string(value: Any) -> str:
    """Render a value as the string the server uses for match comparison."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    return str(value)


class BlacklistRule:
    __slots__ = ("schema_hash", "match_fields")

    def __init__(self, schema_hash: str, match_fields: Dict[str, str]) -> None:
        self.schema_hash = schema_hash
        self.match_fields = match_fields

    def _is_wildcard(self) -> bool:
        return self.schema_hash == WILDCARD

    def matches(self, schema_hash: str, data: Dict[str, Any]) -> bool:
        if not self._is_wildcard() and self.schema_hash != schema_hash:
            return False
        for key, expected in self.match_fields.items():
            if key not in data or _to_match_string(data[key]) != expected:
                return False
        return True

    def dedup_key(self) -> str:
        pairs = sorted(self.match_fields.items())
        return self.schema_hash + "".join(f"\x1f{k}={v}" for k, v in pairs)


class Blacklist:
    def __init__(self) -> None:
        self._rules: List[BlacklistRule] = []

    def set_rules(self, rules: List[BlacklistRule]) -> None:
        """Replace all rules (used after the initial fetch)."""
        self._rules = list(rules)

    def merge_rules(self, rules: List[BlacklistRule]) -> None:
        """Merge additional rules in, de-duplicating against existing ones."""
        seen = {r.dedup_key() for r in self._rules}
        for rule in rules:
            key = rule.dedup_key()
            if key not in seen:
                seen.add(key)
                self._rules.append(rule)

    def is_blacklisted(self, schema_hash: str, data: Dict[str, Any]) -> bool:
        return any(r.matches(schema_hash, data) for r in self._rules)

    def __len__(self) -> int:
        return len(self._rules)


def parse_rules(rules: Any) -> List[BlacklistRule]:
    """Parse the ``rules`` array of a blacklist response, or the ``blacklisted``
    array of an upload response. Both share the ``{schema_hash, match}`` shape.
    """
    if not isinstance(rules, list):
        return []
    out: List[BlacklistRule] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        schema_hash = rule.get("schema_hash")
        if not isinstance(schema_hash, str):
            continue
        match_fields: Dict[str, str] = {}
        match = rule.get("match")
        if isinstance(match, dict):
            for k, v in match.items():
                match_fields[k] = _to_match_string(v)
        out.append(BlacklistRule(schema_hash, match_fields))
    return out

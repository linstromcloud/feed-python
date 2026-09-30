"""Client-side blacklist: rules fetched at session start (and updated from
upload responses) that drop matching events before they are sent.

A rule matches an event when its ``schema_hash`` equals the event's hash (or is
the wildcard ``"*"``), and — if it has a ``match`` filter — every condition
matches the ``data`` key of the same name. Conditions arrive as canonical text
and are compared by the field's declared schema type, as the duckfeed
server's docs/client-api.md specifies: ``int64`` and ``float64`` compare
numbers parsed from the text, ``bool`` compares ``true``/``false``, ``string``
compares the text exactly. A nested condition object matches a struct field
whose every conditioned member matches. An absent (``None``) value matches no
condition.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Union

WILDCARD = "*"

MatchValue = Union[str, Dict[str, "MatchValue"]]

_INTEGER = re.compile(r"-?[0-9]+")
_FLOAT = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1
_DECLARED = ("int64", "float64", "bool", "string")


def _parse_match_value(value: Any) -> Optional[MatchValue]:
    """Return a text condition, or a nested condition as a dict of them; ``None``
    for a value a server never sends (a non-string scalar, ``null``, an array)."""
    if isinstance(value, dict):
        nested: Dict[str, MatchValue] = {}
        for k, v in value.items():
            parsed = _parse_match_value(v)
            if parsed is None:
                return None
            nested[k] = parsed
        return nested
    if isinstance(value, str):
        return value
    return None


def _text_matches(text: str, kind: str, value: Any) -> bool:
    if kind == "int64":
        if (
            _INTEGER.fullmatch(text) is None
            or not isinstance(value, int)
            or isinstance(value, bool)
        ):
            return False
        digits = text.lstrip("-").lstrip("0")
        if len(digits) > 19:  # beyond int64, and bounds int()'s digit limit
            return False
        number = int(digits or "0") * (-1 if text.startswith("-") else 1)
        return _INT64_MIN <= number <= _INT64_MAX and number == value
    if kind == "float64":
        return (
            _FLOAT.fullmatch(text) is not None
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and float(text) == value
        )
    if kind == "bool":
        return text in ("true", "false") and value is (text == "true")
    return isinstance(value, str) and value == text


def _condition_matches(condition: MatchValue, field_type: Any, value: Any) -> bool:
    while isinstance(field_type, dict) and "$optional" in field_type:
        field_type = field_type["$optional"]
    if value is None:
        return False
    if isinstance(condition, dict):
        return (
            isinstance(field_type, dict)
            and isinstance(value, dict)
            and all(
                k in value and _condition_matches(c, field_type.get(k), value[k])
                for k, c in condition.items()
            )
        )
    if isinstance(field_type, str) and field_type in _DECLARED:
        kind = field_type
    elif isinstance(field_type, (dict, list)):
        return False
    elif isinstance(value, bool):
        kind = "bool"
    elif isinstance(value, str):
        kind = "string"
    elif isinstance(value, int):
        kind = "int64"
    elif isinstance(value, float):
        kind = "float64"
    else:
        return False
    return _text_matches(condition, kind, value)


class BlacklistRule:
    __slots__ = ("schema_hash", "match_fields")

    def __init__(self, schema_hash: str, match_fields: Dict[str, MatchValue]) -> None:
        self.schema_hash = schema_hash
        self.match_fields = match_fields

    def _is_wildcard(self) -> bool:
        return self.schema_hash == WILDCARD

    def matches(self, schema_hash: str, schema_def: Any, data: Dict[str, Any]) -> bool:
        if not self._is_wildcard() and self.schema_hash != schema_hash:
            return False
        return _condition_matches(self.match_fields, schema_def, data)

    def dedup_key(self) -> str:
        return json.dumps([self.schema_hash, self.match_fields], sort_keys=True)


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

    def is_blacklisted(
        self, schema_hash: str, schema_def: Any, data: Dict[str, Any]
    ) -> bool:
        return any(r.matches(schema_hash, schema_def, data) for r in self._rules)

    def __len__(self) -> int:
        return len(self._rules)


def parse_rules(rules: Any) -> List[BlacklistRule]:
    """Parse the ``rules`` array of a blacklist response, or the ``blacklisted``
    array of an upload response. Both share the ``{schema_hash, match}`` shape.
    A rule with a match value a server never sends is left out.
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
        match_fields: Optional[MatchValue] = {}
        match = rule.get("match")
        if isinstance(match, dict):
            match_fields = _parse_match_value(match)
        if isinstance(match_fields, dict):
            out.append(BlacklistRule(schema_hash, match_fields))
    return out

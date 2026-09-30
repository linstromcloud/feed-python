"""Shared blacklist matching vectors (tests/data/blacklist-match-vectors.json,
a copy of the duckfeed server's docs/blacklist-match-vectors.json)."""

import json
from pathlib import Path

import pytest

from feed.blacklist import Blacklist, parse_rules

VECTORS = json.loads(
    (Path(__file__).parent / "data" / "blacklist-match-vectors.json").read_text()
)["vectors"]


def _text(condition):
    """Condition leaves as a server sends them: canonical text."""
    if isinstance(condition, dict):
        return {k: _text(v) for k, v in condition.items()}
    if isinstance(condition, bool):
        return "true" if condition else "false"
    if isinstance(condition, float) and condition.is_integer():
        return str(int(condition))
    return str(condition)


def _received(vector):
    """The condition as a client receives it, or None when never sent."""
    if "text" in vector:
        return vector["text"]
    # Servers before duckfeed#603 also sent struct member conditions as text.
    if isinstance(vector["condition"], dict) and "null" not in json.dumps(
        vector["condition"]
    ):
        return _text(vector["condition"])
    return None


@pytest.mark.parametrize(
    "vector",
    [v for v in VECTORS if _received(v) is not None],
    ids=lambda v: json.dumps(v, sort_keys=True),
)
def test_vector(vector):
    bl = Blacklist()
    bl.set_rules(parse_rules([{"schema_hash": "*", "match": {"f": _received(vector)}}]))
    data = {"f": vector["value"]} if "value" in vector else {}
    assert bl.is_blacklisted("h", {"f": vector["type"]}, data) is vector["match"]

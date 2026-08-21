from feed.schema import canonical_json_string, compute_schema_hash


def test_enemy_kill_event_canonical_form():
    schema = {
        "enemy_id": "int64",
        "position": {"x": "float64", "y": "float64", "z": "float64"},
        "weapons_used": ["string"],
        "assist_id": {"$optional": "int64"},
    }
    expected = (
        '{"assist_id":{"$optional":"int64"},"enemy_id":"int64",'
        '"position":{"x":"float64","y":"float64","z":"float64"},'
        '"weapons_used":["string"]}'
    )
    assert canonical_json_string(schema) == expected


def test_key_order_does_not_change_hash():
    a = {"b": "int64", "a": "string"}
    b = {"a": "string", "b": "int64"}
    assert compute_schema_hash(a) == compute_schema_hash(b)


def test_case_insensitive_sort_matches_server():
    schema = {
        "$schema_name": "Events",
        "Branch": "string",
        "Category": "string",
        "discord_id": "int64",
        "Duration": "float64",
    }
    assert canonical_json_string(schema) == (
        '{"$schema_name":"Events","Branch":"string","Category":"string",'
        '"discord_id":"int64","Duration":"float64"}'
    )


def test_protocol_hash_vector():
    """A fixed vector detects accidental canonicalization changes."""
    schema = {
        "$schema_name": "events",
        "branch": "string",
        "category": "string",
        "duration": "float64",
        "name": "string",
        "pirated": "bool",
        "time": "float64",
        "version": "string",
        "discord_id": "int64",
        "event_type": "string",
        "build_version": "string",
        "linked_def": "string",
        "new_value_i32": "int64",
        "prev_value_i32": "int64",
        "run_id": "string",
        "session_id": "string",
        "session_start_unixtime": "int64",
        "steam_id": "int64",
        "ui_id": "int64",
    }
    assert (
        compute_schema_hash(schema)
        == "c75bad170717eff8ca505edbb17ff0e800dc50a8d34c02bcd58de158eeb26d7b"
    )

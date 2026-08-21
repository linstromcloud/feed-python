import pytest

from feed.blacklist import Blacklist, parse_rules
from feed.fields import EventBuilder, FieldType
from feed.protocol import build_event
from feed.state import StateStore, merge


# --- fields ---------------------------------------------------------------


def test_inference_and_lowercasing():
    fields = (
        EventBuilder()
        .add("EnemyType", "goblin")
        .add("Wave", 3)
        .add("Damage", 4.5)
        .add("Crit", True)
        .build()
    )
    assert [f.name for f in fields] == ["enemytype", "wave", "damage", "crit"]
    assert [f.ftype for f in fields] == [
        FieldType.STRING,
        FieldType.INT64,
        FieldType.FLOAT64,
        FieldType.BOOL,
    ]


def test_bool_not_treated_as_int():
    f = EventBuilder().add("flag", True).build()[0]
    assert f.ftype is FieldType.BOOL


def test_optional_and_array_descriptors():
    fields = (
        EventBuilder().add_optional_int("assist", None).add("tags", ["a", "b"]).build()
    )
    assert fields[0].type_descriptor() == {"$optional": "int64"}
    assert fields[0].data_value() is None
    assert fields[1].type_descriptor() == ["string"]


def test_variant_preserves_dynamic_json_shape_under_one_descriptor():
    field = (
        EventBuilder()
        .add_variant(
            "Config",
            {
                "Model": {"Width": 64},
                "schedule": [{"step": 0, "lr": 0.1}],
                "optional": None,
                "empty": [],
            },
        )
        .build()[0]
    )
    assert field.ftype is FieldType.VARIANT
    assert field.type_descriptor() == "variant"
    assert field.data_value()["Model"] == {"Width": 64}


def test_variant_rejects_non_json_values():
    with pytest.raises(TypeError):
        EventBuilder().add_variant("config", {"bad": object()})


def test_variant_shape_does_not_change_schema_hash():
    first_hash, first_schema, _ = build_event(
        "run",
        EventBuilder().add_variant("config", {"model": {"width": 64}}).build(),
    )
    second_hash, second_schema, _ = build_event(
        "run",
        EventBuilder()
        .add_variant("config", {"dataset": "cifar10", "schedule": [0.1, 0.01]})
        .build(),
    )
    assert (
        first_schema
        == second_schema
        == {
            "$schema_name": "run",
            "config": "variant",
        }
    )
    assert first_hash == second_hash


def test_infer_none_raises():
    with pytest.raises(ValueError):
        EventBuilder().add("x", None)


def test_infer_empty_list_raises():
    with pytest.raises(ValueError):
        EventBuilder().add("x", [])


def test_nested_struct_and_array_of_structs():
    fields = (
        EventBuilder()
        .add(
            "Summary",
            {
                "Mean": 0.5,
                "Bins": [1, 2],
                "Items": [{"Name": "a", "Score": 1.0}, {"Name": "b", "Score": 2.0}],
            },
        )
        .build()
    )
    assert fields[0].type_descriptor() == {
        "mean": "float64",
        "bins": ["int64"],
        "items": [{"name": "string", "score": "float64"}],
    }
    assert fields[0].data_value()["items"][0] == {"name": "a", "score": 1.0}


def test_nested_arrays_must_be_homogeneous():
    with pytest.raises(TypeError):
        EventBuilder().add("items", [{"x": 1}, {"x": "different"}])


@pytest.mark.parametrize("values", ([1, 2.5], [1, True], ["1", 2]))
def test_primitive_arrays_must_be_homogeneous(values):
    with pytest.raises(TypeError):
        EventBuilder().add("values", values)


@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf")))
def test_non_finite_floats_are_rejected(value):
    with pytest.raises(ValueError, match="finite"):
        EventBuilder().add("value", value)


def test_integers_must_fit_int64():
    with pytest.raises(OverflowError, match="int64"):
        EventBuilder().add("value", 1 << 63)


def test_duplicate_names_after_lowercasing_are_rejected():
    with pytest.raises(ValueError, match="duplicate field name"):
        EventBuilder().add("Loss", 1.0).add("loss", 0.5).build()

    with pytest.raises(ValueError, match="duplicate nested field name"):
        EventBuilder().add("summary", {"Mean": 1.0, "mean": 2.0})


@pytest.mark.parametrize("name", ("", "train loss", "train-loss", "métrique"))
def test_field_names_are_sql_friendly(name):
    with pytest.raises(ValueError, match="ASCII letters"):
        EventBuilder().add(name, 1)


# --- state ----------------------------------------------------------------


def test_state_set_replaces():
    s = StateStore()
    s.set("a", FieldType.INT64, 1)
    s.set("a", FieldType.INT64, 2)
    snap = s.snapshot()
    assert len(snap) == 1 and snap[0].value == 2


def test_state_snapshot_isolated():
    s = StateStore()
    s.set("a", FieldType.INT64, 1)
    snap = s.snapshot()
    s.set("b", FieldType.INT64, 2)
    assert len(snap) == 1  # captured snapshot unaffected


def test_state_case_insensitive():
    s = StateStore()
    s.set("Score", FieldType.INT64, 1)
    s.set("SCORE", FieldType.INT64, 2)
    snap = s.snapshot()
    assert len(snap) == 1 and snap[0].name == "score" and snap[0].value == 2
    assert s.has("ScOrE")
    s.remove("sCoRe")
    assert not s.has("score")


def test_merge_event_shadows_state_case_insensitively():
    state = StateStore()
    state.set("Score", FieldType.INT64, 1)
    event = EventBuilder().add("score", 99).build()
    merged = merge(state.snapshot(), event)
    assert len(merged) == 1
    assert merged[0].name == "score" and merged[0].value == 99


# --- blacklist ------------------------------------------------------------


def test_hash_only_rule():
    bl = Blacklist()
    bl.set_rules(parse_rules([{"schema_hash": "abc"}]))
    assert bl.is_blacklisted("abc", {"x": 1})
    assert not bl.is_blacklisted("def", {"x": 1})


def test_match_filter_stringified():
    bl = Blacklist()
    bl.set_rules(
        parse_rules(
            [{"schema_hash": "abc", "match": {"event_type": "shoot", "damage": "12.5"}}]
        )
    )
    assert bl.is_blacklisted("abc", {"event_type": "shoot", "damage": 12.5})
    assert not bl.is_blacklisted("abc", {"event_type": "shoot", "damage": 13.0})
    assert not bl.is_blacklisted("abc", {"event_type": "shoot"})  # missing field


def test_wildcard():
    bl = Blacklist()
    bl.set_rules(
        parse_rules([{"schema_hash": "*", "match": {"build_version": "1.4.2"}}])
    )
    assert bl.is_blacklisted("anything", {"build_version": "1.4.2"})
    assert not bl.is_blacklisted("anything", {"build_version": "1.5.0"})


def test_merge_dedups():
    bl = Blacklist()
    bl.set_rules(parse_rules([{"schema_hash": "abc"}]))
    bl.merge_rules(parse_rules([{"schema_hash": "abc"}, {"schema_hash": "xyz"}]))
    assert len(bl) == 2


# --- protocol -------------------------------------------------------------


def test_build_event_lowercases_and_hashes_consistently():
    mixed_hash, schema_def, data = build_event(
        "EnemyKilled", EventBuilder().add("EnemyType", "goblin").add("Wave", 3).build()
    )
    lower_hash, _, _ = build_event(
        "enemykilled", EventBuilder().add("enemytype", "goblin").add("wave", 3).build()
    )
    assert mixed_hash == lower_hash
    assert schema_def["$schema_name"] == "enemykilled"
    assert "enemytype" in schema_def and "EnemyType" not in schema_def
    assert data["wave"] == 3

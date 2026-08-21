"""Typed telemetry field values and the event builder.

The Feed type system is ``bool``, ``int64``, ``float64``, ``string``,
``variant``, plus a homogeneous array and an optional (nullable) form of each
scalar. Each field knows both its *type descriptor* (used to build the schema
that gets hashed) and its *data value* (the actual payload).

Python's ``int``/``float`` distinction maps cleanly to ``int64``/``float64``;
``bool`` is checked before ``int`` since ``bool`` is a subclass of ``int``.
"""

from __future__ import annotations

import json
import math
import re
from numbers import Integral, Real
from enum import Enum
from typing import Any, List, Optional


_NAME_RE = re.compile(r"[a-z0-9_]+")
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


class FieldType(Enum):
    BOOL = "bool"
    INT64 = "int64"
    FLOAT64 = "float64"
    STRING = "string"
    VARIANT = "variant"
    BOOL_ARRAY = "bool[]"
    INT64_ARRAY = "int64[]"
    FLOAT64_ARRAY = "float64[]"
    STRING_ARRAY = "string[]"
    OPTIONAL_BOOL = "bool?"
    OPTIONAL_INT64 = "int64?"
    OPTIONAL_FLOAT64 = "float64?"
    OPTIONAL_STRING = "string?"
    STRUCT = "struct"
    ARRAY = "array"


_SCALAR_DESCRIPTOR = {
    FieldType.BOOL: "bool",
    FieldType.INT64: "int64",
    FieldType.FLOAT64: "float64",
    FieldType.STRING: "string",
    FieldType.VARIANT: "variant",
}
_ARRAY_INNER = {
    FieldType.BOOL_ARRAY: "bool",
    FieldType.INT64_ARRAY: "int64",
    FieldType.FLOAT64_ARRAY: "float64",
    FieldType.STRING_ARRAY: "string",
}
_OPTIONAL_INNER = {
    FieldType.OPTIONAL_BOOL: "bool",
    FieldType.OPTIONAL_INT64: "int64",
    FieldType.OPTIONAL_FLOAT64: "float64",
    FieldType.OPTIONAL_STRING: "string",
}


class Field:
    """A named field: a column name plus its typed value.

    The name is lowercased (ASCII) at construction so that field names compare
    case-insensitively throughout the protocol. This is the single normalization
    point for every construction path.
    """

    __slots__ = ("name", "ftype", "value", "_descriptor")

    def __init__(
        self,
        name: str,
        ftype: FieldType,
        value: Any,
        descriptor: Any = None,
    ) -> None:
        self.name = _normalize_name(name, "field name")
        self.ftype = ftype
        self.value = value
        self._descriptor = descriptor

    def type_descriptor(self) -> Any:
        """Schema type descriptor as a JSON-serializable value.

        Primitives are strings (``"int64"``), arrays are single-element arrays
        (``["int64"]``), optionals are ``{"$optional": <inner>}``.
        """
        if self._descriptor is not None:
            return self._descriptor
        if self.ftype in _SCALAR_DESCRIPTOR:
            return _SCALAR_DESCRIPTOR[self.ftype]
        if self.ftype in _ARRAY_INNER:
            return [_ARRAY_INNER[self.ftype]]
        return {"$optional": _OPTIONAL_INNER[self.ftype]}

    def data_value(self) -> Any:
        """The data payload value (already JSON-serializable)."""
        return self.value

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"Field({self.name!r}, {self.ftype.name}, {self.value!r})"


class EventBuilder:
    """Internal builder for one record's typed fields.

    >>> fields = (
    ...     EventBuilder()
    ...     .add("iteration", 20)
    ...     .add("throughput", 412.8)
    ...     .add("converged", True)
    ...     .build()
    ... )
    >>> len(fields)
    3

    ``add`` infers the type from the Python value. For explicit control (e.g.
    forcing ``float64`` for a whole number) use the typed ``add_*`` methods. For
    nullable columns use ``add_optional_*`` so the type is known even when the
    value is ``None``.
    """

    __slots__ = ("_fields",)

    def __init__(self) -> None:
        self._fields: List[Field] = []

    # --- inferred ---------------------------------------------------------

    def add(self, name: str, value: Any) -> "EventBuilder":
        """Add a field, inferring its type from ``value``.

        ``bool`` -> bool, ``int`` -> int64, ``float`` -> float64, ``str`` ->
        string, ``list`` -> homogeneous array. Empty lists and ``None`` raise
        because they do not carry enough information to infer a wire type.
        """
        self._fields.append(_infer_field(name, value))
        return self

    # --- explicit scalars -------------------------------------------------

    def add_bool(self, name: str, value: bool) -> "EventBuilder":
        self._fields.append(Field(name, FieldType.BOOL, _require_bool(value, name)))
        return self

    def add_int(self, name: str, value: int) -> "EventBuilder":
        self._fields.append(Field(name, FieldType.INT64, _normalize_int(value, name)))
        return self

    def add_float(self, name: str, value: float) -> "EventBuilder":
        self._fields.append(
            Field(name, FieldType.FLOAT64, _normalize_float(value, name))
        )
        return self

    def add_string(self, name: str, value: str) -> "EventBuilder":
        self._fields.append(Field(name, FieldType.STRING, _require_string(value, name)))
        return self

    def add_variant(self, name: str, value: Any) -> "EventBuilder":
        """Add an arbitrary JSON-compatible value under a stable schema type."""
        try:
            normalized = json.loads(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError) as error:
            raise TypeError(
                f"variant field {name!r} must be JSON-compatible"
            ) from error
        self._fields.append(Field(name, FieldType.VARIANT, normalized))
        return self

    # --- explicit arrays --------------------------------------------------

    def add_bool_array(self, name: str, values: List[bool]) -> "EventBuilder":
        normalized = [_require_bool(value, f"{name}[]") for value in values]
        self._fields.append(Field(name, FieldType.BOOL_ARRAY, normalized))
        return self

    def add_int_array(self, name: str, values: List[int]) -> "EventBuilder":
        normalized = [_normalize_int(value, f"{name}[]") for value in values]
        self._fields.append(Field(name, FieldType.INT64_ARRAY, normalized))
        return self

    def add_float_array(self, name: str, values: List[float]) -> "EventBuilder":
        normalized = [_normalize_float(value, f"{name}[]") for value in values]
        self._fields.append(Field(name, FieldType.FLOAT64_ARRAY, normalized))
        return self

    def add_string_array(self, name: str, values: List[str]) -> "EventBuilder":
        normalized = [_require_string(value, f"{name}[]") for value in values]
        self._fields.append(Field(name, FieldType.STRING_ARRAY, normalized))
        return self

    # --- explicit optionals ----------------------------------------------

    def add_optional_bool(self, name: str, value: Optional[bool]) -> "EventBuilder":
        normalized = None if value is None else _require_bool(value, name)
        self._fields.append(Field(name, FieldType.OPTIONAL_BOOL, normalized))
        return self

    def add_optional_int(self, name: str, value: Optional[int]) -> "EventBuilder":
        normalized = None if value is None else _normalize_int(value, name)
        self._fields.append(Field(name, FieldType.OPTIONAL_INT64, normalized))
        return self

    def add_optional_float(self, name: str, value: Optional[float]) -> "EventBuilder":
        normalized = None if value is None else _normalize_float(value, name)
        self._fields.append(Field(name, FieldType.OPTIONAL_FLOAT64, normalized))
        return self

    def add_optional_string(self, name: str, value: Optional[str]) -> "EventBuilder":
        normalized = None if value is None else _require_string(value, name)
        self._fields.append(Field(name, FieldType.OPTIONAL_STRING, normalized))
        return self

    def build(self) -> List[Field]:
        """Return the accumulated fields (and reset the builder)."""
        seen = set()
        for item in self._fields:
            if item.name in seen:
                raise ValueError(
                    f"duplicate field name after lowercasing: {item.name!r}"
                )
            seen.add(item.name)
        out = self._fields
        self._fields = []
        return out


def _infer_field(name: str, value: Any) -> Field:
    # bool must precede int: bool is a subclass of int in Python.
    if isinstance(value, bool):
        return Field(name, FieldType.BOOL, value)
    if isinstance(value, Integral):
        return Field(name, FieldType.INT64, _normalize_int(value, name))
    if isinstance(value, Real):
        return Field(name, FieldType.FLOAT64, _normalize_float(value, name))
    if isinstance(value, str):
        return Field(name, FieldType.STRING, value)
    if isinstance(value, dict):
        descriptor, normalized = _infer_nested(value, name)
        return Field(name, FieldType.STRUCT, normalized, descriptor)
    if isinstance(value, (list, tuple)):
        return _infer_array(name, list(value))
    if value is None:
        raise ValueError(
            f"cannot infer type of None for field {name!r}; use an add_optional_* method"
        )
    raise TypeError(f"unsupported field type for {name!r}: {type(value).__name__}")


def _infer_array(name: str, values: list) -> Field:
    if not values:
        raise ValueError(
            f"cannot infer element type of empty list for field {name!r}; "
            "use an explicit add_*_array method"
        )
    descriptors_and_values = [_infer_nested(value, f"{name}[]") for value in values]
    descriptors = [item[0] for item in descriptors_and_values]
    if any(descriptor != descriptors[0] for descriptor in descriptors[1:]):
        raise TypeError(f"array field {name!r} must contain one homogeneous Feed type")
    normalized = [item[1] for item in descriptors_and_values]
    primitive_type = None
    if isinstance(descriptors[0], str):
        primitive_type = {
            "bool": FieldType.BOOL_ARRAY,
            "int64": FieldType.INT64_ARRAY,
            "float64": FieldType.FLOAT64_ARRAY,
            "string": FieldType.STRING_ARRAY,
        }.get(descriptors[0])
    if primitive_type is not None:
        return Field(name, primitive_type, normalized)
    return Field(
        name,
        FieldType.ARRAY,
        normalized,
        [descriptors[0]],
    )


def _infer_nested(value: Any, path: str):
    """Return a schema descriptor and normalized value recursively."""
    if isinstance(value, bool):
        return "bool", value
    if isinstance(value, Integral):
        return "int64", _normalize_int(value, path)
    if isinstance(value, Real):
        return "float64", _normalize_float(value, path)
    if isinstance(value, str):
        return "string", value
    if isinstance(value, dict):
        descriptor = {}
        normalized = {}
        for key, inner in value.items():
            normalized_key = _normalize_name(key, f"nested field name at {path!r}")
            if normalized_key in descriptor:
                raise ValueError(
                    f"duplicate nested field name after lowercasing at {path!r}: "
                    f"{normalized_key!r}"
                )
            nested_descriptor, nested_value = _infer_nested(inner, f"{path}.{key}")
            descriptor[normalized_key] = nested_descriptor
            normalized[normalized_key] = nested_value
        return descriptor, normalized
    if isinstance(value, (list, tuple)):
        values = list(value)
        if not values:
            raise ValueError(f"cannot infer element type of empty list at {path!r}")
        items = [_infer_nested(item, f"{path}[]") for item in values]
        descriptor = items[0][0]
        if any(item[0] != descriptor for item in items[1:]):
            raise TypeError(f"array at {path!r} must contain one homogeneous Feed type")
        return [descriptor], [item[1] for item in items]
    if value is None:
        raise ValueError(
            f"cannot infer type of None at {path!r}; use an explicit optional field"
        )
    raise TypeError(f"unsupported nested type at {path!r}: {type(value).__name__}")


def _normalize_name(name: str, kind: str) -> str:
    if not isinstance(name, str):
        raise TypeError(f"{kind} must be a string")
    normalized = name.lower()
    if not _NAME_RE.fullmatch(normalized):
        raise ValueError(
            f"{kind} must contain only ASCII letters, digits, and underscores: {name!r}"
        )
    return normalized


def _require_bool(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"field {path!r} must be a bool")
    return value


def _normalize_int(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"field {path!r} must be an integer")
    normalized = int(value)
    if not (_INT64_MIN <= normalized <= _INT64_MAX):
        raise OverflowError(f"integer field {path!r} is outside the int64 range")
    return normalized


def _normalize_float(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"field {path!r} must be a real number")
    normalized = float(value)
    if not math.isfinite(normalized):
        raise ValueError(f"float field {path!r} must be finite")
    return normalized


def _require_string(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"field {path!r} must be a string")
    return value

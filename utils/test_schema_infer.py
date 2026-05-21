"""Tests for utils.schema_infer — native-format schema + profile inference."""

from __future__ import annotations

from datetime import datetime

from bson import ObjectId

from utils.schema_infer import infer


def _types(docs: list[dict]) -> dict[str, str]:
    return {f.name: f.dataType.simpleString() for f in infer(docs).struct.fields}


def test_flat_types_keep_native_camelcase_keys() -> None:
    docs = [
        {"authorId": "n1", "count": 3, "ratio": 1.5, "ok": True},
        {"authorId": "n2", "count": 7, "ratio": 2.0, "ok": False},
    ]
    types = _types(docs)
    assert types == {
        "authorId": "string",
        "count": "bigint",
        "ratio": "double",
        "ok": "boolean",
    }
    # camelCase is preserved — never normalized to snake_case.
    assert "author_id" not in types


def test_absent_field_is_nullable() -> None:
    result = infer([{"a": 1, "b": 2}, {"a": 1}])
    by_name = {f.name: f for f in result.struct.fields}
    assert by_name["a"].nullable is False
    assert by_name["b"].nullable is True
    assert result.profile["b"]["null_fraction"] == 0.5


def test_none_value_makes_field_nullable() -> None:
    result = infer([{"a": 1}, {"a": None}])
    assert {f.name: f.nullable for f in result.struct.fields}["a"] is True


def test_nested_dict_becomes_struct() -> None:
    result = infer([{"meta": {"k": "v", "n": 1}}, {"meta": {"k": "w", "n": 2}}])
    meta = {f.name: f for f in result.struct.fields}["meta"]
    assert meta.dataType.simpleString() == "struct<k:string,n:bigint>"
    assert result.profile["meta"]["type"] == "struct"


def test_array_of_scalars_and_of_structs() -> None:
    docs = [
        {"tags": ["a", "b"], "items": [{"x": 1}]},
        {"tags": ["c"], "items": [{"x": 2}, {"x": 3}]},
    ]
    types = _types(docs)
    assert types["tags"] == "array<string>"
    assert types["items"] == "array<struct<x:bigint>>"
    assert infer(docs).profile["items"]["max_len"] == 2


def test_empty_array_falls_back_to_string_element() -> None:
    assert _types([{"tags": []}, {"tags": []}])["tags"] == "array<string>"


def test_mixed_int_float_widens_to_double() -> None:
    assert _types([{"v": 1}, {"v": 2.5}])["v"] == "double"


def test_mixed_incompatible_falls_back_to_string_with_warning() -> None:
    result = infer([{"v": "text"}, {"v": 5}])
    assert {f.name: f.dataType.simpleString() for f in result.struct.fields}["v"] == (
        "string"
    )
    assert any("mixed" in w for w in result.warnings)


def test_datetime_infers_timestamp() -> None:
    assert _types([{"t": datetime(2024, 1, 1)}, {"t": datetime(2024, 2, 1)}])["t"] == (
        "timestamp"
    )


def test_string_format_detection() -> None:
    assert infer([{"c": "<p>hi</p>"}] * 5).profile["c"]["format"] == "html"
    assert infer([{"c": "6641a3b2e4b0c1234567abcd"}] * 5).profile["c"]["format"] == (
        "hex24"
    )
    assert infer([{"c": "a.b@example.com"}] * 5).profile["c"]["format"] == "email"
    assert infer([{"c": "just some words"}] * 5).profile["c"]["format"] == "plain"


def test_objectid_id_sanitized_to_string() -> None:
    docs = [{"_id": ObjectId(), "x": 1}, {"_id": ObjectId(), "x": 2}]
    assert _types(docs)["_id"] == "string"


def test_string_length_profile() -> None:
    profile = infer([{"c": "ab"}, {"c": "abcd"}]).profile["c"]
    assert profile["min_len"] == 2
    assert profile["max_len"] == 4
    assert profile["avg_len"] == 3

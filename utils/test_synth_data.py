"""Tests for utils.synth_data — realistic, synthetic-only row generation."""

from __future__ import annotations

from utils.schema_infer import infer
from utils.synth_data import generate


def test_rows_are_schema_complete() -> None:
    # 'b' is absent from the second doc — every generated row must still
    # carry every field (create_dataframe rejects rows missing a field).
    result = infer([{"a": 1, "b": "x"}, {"a": 2}])
    rows = generate(result.struct, result.profile, n=10)
    assert len(rows) == 10
    for row in rows:
        assert set(row) == {"a", "b"}


def test_deterministic_for_a_given_seed() -> None:
    result = infer([{"a": 1, "c": "hello world"}])
    first = generate(result.struct, result.profile, n=5, seed=7)
    second = generate(result.struct, result.profile, n=5, seed=7)
    assert first == second


def test_html_field_generates_realistic_large_html() -> None:
    big_html = "<p>" + "clinical note " * 40 + "</p>"
    result = infer([{"content": big_html}] * 5)
    for row in generate(result.struct, result.profile, n=5):
        assert "<" in row["content"] and ">" in row["content"]
        assert len(row["content"]) > 100


def test_no_verbatim_real_values_are_propagated() -> None:
    secret = "patient Jane Doe SSN 123456789 confidential"
    result = infer([{"content": secret}] * 5)
    rows = generate(result.struct, result.profile, n=20)
    assert all(row["content"] != secret for row in rows)


def test_numeric_values_within_observed_range() -> None:
    result = infer([{"n": 10}, {"n": 20}, {"n": 30}])
    rows = generate(result.struct, result.profile, n=50)
    assert all(10 <= row["n"] <= 30 for row in rows)


def test_all_null_field_generates_none() -> None:
    result = infer([{"a": 1, "maybe": None}, {"a": 2, "maybe": None}])
    rows = generate(result.struct, result.profile, n=5)
    assert all(row["maybe"] is None for row in rows)


def test_nested_struct_and_array_are_generated() -> None:
    result = infer(
        [
            {"meta": {"k": "v"}, "tags": ["a", "b"]},
            {"meta": {"k": "w"}, "tags": ["c"]},
        ]
    )
    for row in generate(result.struct, result.profile, n=5):
        assert isinstance(row["meta"], dict) and "k" in row["meta"]
        assert isinstance(row["tags"], list)

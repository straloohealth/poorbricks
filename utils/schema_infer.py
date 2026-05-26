"""Infer a Spark schema + per-field profile from raw MongoDB documents.

Native format: field names are kept exactly as MongoDB stores them
(camelCase, ``_id``, ...). Nothing is normalized to snake_case — a
``verify --mode db`` run must feed each pipeline data shaped exactly like
production, so format-handling bugs surface instead of being masked.

The profile records, per field, enough shape detail (string length bands,
detected text format, numeric ranges, array lengths) for ``utils.synth_data``
to generate realistic example rows. Free-text and high-cardinality values
never reach the profile — the one deliberate exception is low-cardinality
enum-like fields, whose small set of short, repeated values is recorded as
``categories`` so synthetic rows stay valid against enum ValidationRules.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from utils.mongo import _sanitize_value

MAX_DEPTH = 6

_HTML_RE = re.compile(r"<[a-zA-Z/!][^>]*>")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-" r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_HEX24_RE = re.compile(r"^[0-9a-fA-F]{24}$")

Profile = dict[str, Any]

# A plain-text string field whose sampled values collapse to a small set of
# short, repeated distinct values is treated as an enum: its real value set is
# recorded so synth_data can draw guaranteed-valid examples. The guards keep
# free text and PII (high-cardinality or long values) out of the profile.
_MAX_CATEGORIES = 30
_MAX_CATEGORY_LEN = 40


@dataclass
class InferResult:
    """Inferred shape of a MongoDB collection."""

    struct: StructType
    profile: dict[str, Profile]
    warnings: list[str] = field(default_factory=list)


def infer(docs: list[dict[str, Any]]) -> InferResult:
    """Infer a native-format Spark schema + field profile from sampled docs."""
    sanitized = [_sanitize_value(d) for d in docs]
    total = len(sanitized)
    names = sorted({k for d in sanitized for k in d})
    warnings: list[str] = []
    fields: list[StructField] = []
    profile: dict[str, Profile] = {}
    for name in names:
        present = [d[name] for d in sanitized if name in d]
        non_null = [v for v in present if v is not None]
        nullable = len(non_null) < total
        null_fraction = round((total - len(non_null)) / total, 4) if total else 1.0
        dtype, fprof = _infer(non_null, depth=0, path=name, warnings=warnings)
        fprof["nullable"] = nullable
        fprof["null_fraction"] = null_fraction
        fields.append(StructField(name, dtype, nullable))
        profile[name] = fprof
    return InferResult(StructType(fields), profile, warnings)


def _classify(value: Any) -> str:
    """Bucket a Python value into a coarse type tag (bool before int)."""
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, datetime):
        return "datetime"
    if isinstance(value, date):
        return "date"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return "other"


def _empty_string_profile() -> Profile:
    return {
        "type": "string",
        "format": "plain",
        "min_len": 0,
        "max_len": 0,
        "avg_len": 0,
    }


def _infer(
    values: list[Any], depth: int, path: str, warnings: list[str]
) -> tuple[DataType, Profile]:
    """Infer one field's Spark type + profile from its observed values."""
    if not values:
        return StringType(), _empty_string_profile()
    classes = {_classify(v) for v in values}

    if classes == {"dict"}:
        if depth >= MAX_DEPTH:
            warnings.append(
                f"{path}: nesting exceeds depth {MAX_DEPTH}; treated as string"
            )
            prof = _empty_string_profile()
            prof["format"] = "json"
            return StringType(), prof
        return _infer_struct(values, depth, path, warnings)

    if classes == {"list"}:
        return _infer_array(values, depth, path, warnings)

    if classes <= {"int", "float"}:
        nums = [float(v) for v in values]
        is_double = "float" in classes
        dtype: DataType = DoubleType() if is_double else LongType()
        return dtype, {
            "type": "double" if is_double else "long",
            "min": min(nums),
            "max": max(nums),
        }

    if classes == {"bool"}:
        return BooleanType(), {"type": "boolean"}

    if classes <= {"datetime", "date"}:
        if "datetime" in classes:
            return TimestampType(), {"type": "timestamp"}
        return DateType(), {"type": "date"}

    if classes == {"str"}:
        return StringType(), _string_profile([v for v in values if isinstance(v, str)])

    # Mixed / unsupported types — lossy String fallback.
    warnings.append(f"{path}: mixed types {sorted(classes)}; coerced to string")
    strs = [v if isinstance(v, str) else json.dumps(v, default=str) for v in values]
    prof = _string_profile(strs)
    prof["mixed"] = sorted(classes)
    return StringType(), prof


def _infer_struct(
    values: list[Any], depth: int, path: str, warnings: list[str]
) -> tuple[StructType, Profile]:
    dicts = [v for v in values if isinstance(v, dict)]
    total = len(dicts)
    subnames = sorted({k for d in dicts for k in d})
    fields: list[StructField] = []
    subprofile: dict[str, Profile] = {}
    for name in subnames:
        present = [d[name] for d in dicts if name in d]
        non_null = [v for v in present if v is not None]
        nullable = len(non_null) < total
        dtype, fprof = _infer(non_null, depth + 1, f"{path}.{name}", warnings)
        fprof["nullable"] = nullable
        fprof["null_fraction"] = (
            round((total - len(non_null)) / total, 4) if total else 1.0
        )
        fields.append(StructField(name, dtype, nullable))
        subprofile[name] = fprof
    return StructType(fields), {"type": "struct", "fields": subprofile}


def _infer_array(
    values: list[Any], depth: int, path: str, warnings: list[str]
) -> tuple[ArrayType, Profile]:
    lists = [v for v in values if isinstance(v, list)]
    lengths = [len(x) for x in lists]
    elements = [e for x in lists for e in x if e is not None]
    if elements:
        elem_type, elem_prof = _infer(elements, depth + 1, f"{path}[]", warnings)
    else:
        elem_type, elem_prof = StringType(), _empty_string_profile()
    return ArrayType(elem_type, True), {
        "type": "array",
        "min_len": min(lengths) if lengths else 0,
        "max_len": max(lengths) if lengths else 0,
        "element": elem_prof,
    }


def _string_profile(values: list[str]) -> Profile:
    lengths = [len(v) for v in values] or [0]
    fmt = _detect_format(values)
    prof: Profile = {
        "type": "string",
        "min_len": min(lengths),
        "max_len": max(lengths),
        "avg_len": round(sum(lengths) / len(lengths)),
        "format": fmt,
    }
    categories = _categorical_values(values) if fmt == "plain" else None
    if categories is not None:
        prof["categories"] = categories
    return prof


def _categorical_values(values: list[str]) -> list[str] | None:
    """Return the sorted distinct values when a plain-text field is a
    low-cardinality enum (status, channel, role, ...).

    Enum-like fields must be represented by their real value set so
    ``synth_data`` draws guaranteed-valid examples — otherwise a pipeline's
    enum ``ValidationRule``s reject the synthetic ``verify --mode db`` data.
    A field qualifies only when its sampled values collapse to a few
    (``2..=_MAX_CATEGORIES``), short (``<=_MAX_CATEGORY_LEN`` chars), genuinely
    repeated distinct values; free text, ids and PII fail these guards and
    keep their values out of the profile.
    """
    distinct = set(values)
    if not 2 <= len(distinct) <= _MAX_CATEGORIES:
        return None
    if len(distinct) > len(values) // 2:
        return None
    if any(len(v) > _MAX_CATEGORY_LEN for v in distinct):
        return None
    return sorted(distinct)


def _detect_format(values: list[str]) -> str:
    """Return the dominant text format across a string field's values."""
    sample = values[:200]
    if not sample:
        return "plain"
    counts: dict[str, int] = {}
    for value in sample:
        fmt = _format_of(value)
        counts[fmt] = counts.get(fmt, 0) + 1
    best = max(counts, key=lambda k: counts[k])
    if best != "plain" and counts[best] * 2 > len(sample):
        return best
    return "plain"


def _format_of(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        return "plain"
    if _HTML_RE.search(value):
        return "html"
    if _HEX24_RE.match(stripped):
        return "hex24"
    if _UUID_RE.match(stripped):
        return "uuid"
    if _EMAIL_RE.match(stripped):
        return "email"
    if _URL_RE.match(stripped):
        return "url"
    if stripped[0] in "{[":
        try:
            json.loads(stripped)
            return "json"
        except (ValueError, TypeError):
            pass
    return "plain"


__all__ = ["InferResult", "MAX_DEPTH", "Profile", "infer"]

"""Generate realistic rows from an inferred schema.

Driven by the schema + per-field profile produced by ``utils.schema_infer``.
Generation is synthetic — aggregate shape (length bands, detected text format,
numeric ranges, array lengths) drives free-text and numeric fields, keeping
real production data out of contracts and CI. The one exception is
low-cardinality enum fields: their real value set (recorded by the profiler
as ``categories``) is sampled directly, so generated rows never violate a
pipeline's enum ``ValidationRule``s.

Rows are **schema-complete** — every field is present in every row — so they
satisfy ``utils.dataframes.create_dataframe``'s strict all-fields check.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

Profile = dict[str, Any]

_WORDS = [
    "lorem",
    "ipsum",
    "dolor",
    "sit",
    "amet",
    "consectetur",
    "adipiscing",
    "elit",
    "sed",
    "tempor",
    "incididunt",
    "labore",
    "magna",
    "aliqua",
    "veniam",
    "quis",
    "nostrud",
    "paciente",
    "avaliacao",
    "clinica",
    "registro",
    "consulta",
    "retorno",
    "exame",
    "sintoma",
]
_EPOCH = datetime(2023, 1, 1)
_SPAN_SECONDS = 800 * 24 * 3600


def generate(
    struct: StructType,
    profile: dict[str, Profile],
    n: int = 25,
    seed: int = 20260521,
) -> list[dict[str, Any]]:
    """Generate ``n`` synthetic, schema-complete rows for ``struct``."""
    rng = random.Random(seed)
    return [_gen_struct(struct, profile, rng) for _ in range(n)]


def _gen_struct(
    struct: StructType, profile: dict[str, Profile], rng: random.Random
) -> dict[str, Any]:
    return {
        f.name: _gen_field(f.dataType, profile.get(f.name, {}), rng)
        for f in struct.fields
    }


def _gen_field(dtype: DataType, fprof: Profile, rng: random.Random) -> Any:
    if fprof.get("null_fraction", 0.0) >= 1.0:
        return None
    if isinstance(dtype, StructType):
        return _gen_struct(dtype, fprof.get("fields", {}), rng)
    if isinstance(dtype, ArrayType):
        lo = int(fprof.get("min_len", 0))
        hi = max(lo, int(fprof.get("max_len", lo or 2)))
        element_profile = fprof.get("element", {})
        return [
            _gen_field(dtype.elementType, element_profile, rng)
            for _ in range(rng.randint(lo, hi))
        ]
    if isinstance(dtype, StringType):
        return _gen_string(fprof, rng)
    if isinstance(dtype, BooleanType):
        return rng.random() < 0.5
    if isinstance(dtype, LongType):
        lo, hi = int(fprof.get("min", 0)), int(fprof.get("max", 1000))
        return rng.randint(min(lo, hi), max(lo, hi))
    if isinstance(dtype, DoubleType):
        lo_f = float(fprof.get("min", 0.0))
        hi_f = float(fprof.get("max", 1000.0))
        return round(rng.uniform(min(lo_f, hi_f), max(lo_f, hi_f)), 4)
    if isinstance(dtype, TimestampType):
        return _gen_datetime(rng)
    if isinstance(dtype, DateType):
        return _gen_datetime(rng).date()
    return _gen_string(fprof, rng)


def _gen_datetime(rng: random.Random) -> datetime:
    return _EPOCH + timedelta(seconds=rng.randint(0, _SPAN_SECONDS))


def _gen_string(fprof: Profile, rng: random.Random) -> str:
    # Enum-like field: draw from the real observed value set so the row is
    # always valid against the pipeline's enum ValidationRules.
    categories = fprof.get("categories")
    if categories:
        return str(rng.choice(categories))
    fmt = fprof.get("format", "plain")
    target = max(1, int(fprof.get("avg_len", 12) or 12))
    if fmt == "html":
        return _gen_html(target, rng)
    if fmt == "hex24":
        return "".join(rng.choice("0123456789abcdef") for _ in range(24))
    if fmt == "uuid":
        return str(uuid.UUID(int=rng.getrandbits(128)))
    if fmt == "email":
        return f"{_word(rng)}.{_word(rng)}@example.com"
    if fmt == "url":
        return f"https://example.com/{_word(rng)}/{rng.randint(1, 9999)}"
    if fmt == "json":
        return f'{{"{_word(rng)}": "{_word(rng)}"}}'
    return _gen_text(target, rng)


def _word(rng: random.Random) -> str:
    return rng.choice(_WORDS)


def _gen_text(target: int, rng: random.Random) -> str:
    words: list[str] = []
    length = 0
    while length < target:
        word = _word(rng)
        words.append(word)
        length += len(word) + 1
    return " ".join(words)


def _gen_html(target: int, rng: random.Random) -> str:
    tags = ["p", "div", "span", "li", "h2"]
    parts: list[str] = []
    length = 0
    while length < target:
        tag = rng.choice(tags)
        chunk = f"<{tag}>{_gen_text(rng.randint(20, 90), rng)}</{tag}>"
        parts.append(chunk)
        length += len(chunk)
    return "".join(parts)


__all__ = ["generate"]

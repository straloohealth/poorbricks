"""Tests for literal-column detection and stub wiring into arch checks."""

from __future__ import annotations

from pathlib import Path

from poorbricks.arch import check_architecture
from poorbricks.verification.no_stubs import find_literals_in, literal_columns_for

_CONFIG = """\
from validation import ValidatedStruct


class Out(ValidatedStruct):
    patient_id: str
    country: str
    is_active: bool
"""


def _write_pipeline(d: Path, transform_body: str) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    (d / "config.py").write_text(_CONFIG)
    (d / "transform.py").write_text(transform_body)
    return d / "transform.py"


def test_find_literals_detects_constant_columns(tmp_path: Path) -> None:
    transform = _write_pipeline(
        tmp_path / "t",
        "from pyspark.sql import functions as f\n"
        "def compute(inputs):\n"
        "    return inputs.src.select(\n"
        '        f.col("patient_id"),\n'
        '        f.lit("US").alias("country"),\n'
        '        f.lit(True).alias("is_active"),\n'
        "    )\n",
    )
    findings = find_literals_in(transform)
    cols = {f.column for f in findings}
    assert cols == {"country", "is_active"}
    assert literal_columns_for(transform) == {"country", "is_active"}


def test_find_literals_excludes_lit_none_and_unknown_cols(tmp_path: Path) -> None:
    transform = _write_pipeline(
        tmp_path / "t",
        "from pyspark.sql import functions as f\n"
        "def compute(inputs):\n"
        "    return inputs.src.select(\n"
        '        f.lit(None).cast("string").alias("country"),\n'  # stub, not literal
        '        f.lit("x").alias("not_in_schema"),\n'  # not a schema column
        "    )\n",
    )
    assert find_literals_in(transform) == []


def test_check_architecture_flags_stub(tmp_path: Path) -> None:
    tables = tmp_path / "tables"
    _write_pipeline(
        tables / "silver" / "dim_x",
        "from pyspark.sql import functions as f\n"
        "def compute(inputs):\n"
        "    return inputs.src.select(\n"
        '        f.lit(None).cast("string").alias("country"),\n'
        "    )\n",
    )
    errors = check_architecture(tables_root=tables)
    messages = " ".join(e.message for e in errors)
    assert "STUB_NULL_COLUMN" in messages
    assert "country" in messages

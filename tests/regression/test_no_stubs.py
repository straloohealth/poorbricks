"""Unit tests for poorbricks.verification.no_stubs."""

from __future__ import annotations

from pathlib import Path

from poorbricks.verification import find_stubs_in


def _make_pipeline(tmp_path: Path, transform_src: str, schema_src: str) -> Path:
    pkg = tmp_path / "fake_pipeline"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "config.py").write_text(schema_src)
    transform = pkg / "transform.py"
    transform.write_text(transform_src)
    return transform


_SCHEMA = """
class Thing:
    patient_id: str
    weight_kg: float | None
    height_cm: float | None
"""


def test_lit_none_on_schema_column_is_flagged(tmp_path: Path) -> None:
    transform = _make_pipeline(
        tmp_path,
        transform_src=(
            "from pyspark.sql import functions as f\n"
            "def compute(inputs):\n"
            "    return inputs.select(\n"
            "        f.col('patient_id'),\n"
            "        f.lit(None).cast('double').alias('weight_kg'),\n"
            "    )\n"
        ),
        schema_src=_SCHEMA,
    )
    findings = find_stubs_in(transform)
    assert len(findings) == 1
    assert findings[0].column == "weight_kg"
    assert findings[0].rule == "STUB_NULL_COLUMN"


def test_lit_none_on_non_schema_column_is_ignored(tmp_path: Path) -> None:
    transform = _make_pipeline(
        tmp_path,
        transform_src=(
            "from pyspark.sql import functions as f\n"
            "def compute(inputs):\n"
            "    return inputs.select(\n"
            "        f.col('patient_id'),\n"
            "        f.lit(None).alias('helper_col'),\n"  # helper_col not in schema
            "    )\n"
        ),
        schema_src=_SCHEMA,
    )
    assert find_stubs_in(transform) == []


def test_lit_constant_near_todo_is_flagged(tmp_path: Path) -> None:
    transform = _make_pipeline(
        tmp_path,
        transform_src=(
            "from pyspark.sql import functions as f\n"
            "def compute(inputs):\n"
            "    return inputs.select(\n"
            "        # TODO populate height when source lands\n"
            "        f.lit(0.0).alias('height_cm'),\n"
            "    )\n"
        ),
        schema_src=_SCHEMA,
    )
    findings = find_stubs_in(transform)
    assert len(findings) == 1
    assert findings[0].column == "height_cm"
    assert findings[0].rule == "STUB_CONSTANT"


def test_legitimate_constant_default_is_not_flagged(tmp_path: Path) -> None:
    """``f.lit(False)`` default for a non-null safety boolean without a TODO
    marker shouldn't be flagged — the rule is intentionally conservative."""
    transform = _make_pipeline(
        tmp_path,
        transform_src=(
            "from pyspark.sql import functions as f\n"
            "def compute(inputs):\n"
            "    return inputs.select(\n"
            "        f.coalesce(f.col('archived'), f.lit(False)).alias('archived'),\n"
            "    )\n"
        ),
        schema_src="class Thing:\n    archived: bool\n",
    )
    assert find_stubs_in(transform) == []

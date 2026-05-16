"""Tests for architecture validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from .arch import check_pipeline_dir


@pytest.fixture
def pipeline_dir(tmp_path: Path) -> Path:
    """Create a minimal valid pipeline directory."""
    d = tmp_path / "test_pipeline"
    d.mkdir()
    (d / "__init__.py").write_text("")
    (d / "config.py").write_text(
        "from validation import ValidatedStruct, Expectations\n"
        "class TestModel(ValidatedStruct):\n"
        "    field: str\n"
        "class TestExpectations(Expectations):\n"
        "    UNIQUE_KEYS = [['field']]\n"
    )
    (d / "pipeline.py").write_text(
        "from poorbricks import pipeline, Inputs\n"
        "from pyspark.sql import DataFrame\n"
        "@pipeline(name='test', model=TestModel, level='bronze', comment='test')\n"
        "def test_pipeline(inputs: Inputs) -> DataFrame:\n"
        "    return inputs\n"
    )
    (d / "transform.py").write_text("def compute(inputs): return inputs")
    (d / "fixtures.py").write_text(
        "from poorbricks import scenario\n"
        "@scenario('test')\n"
        "def test_scenario(): pass\n"
    )
    (d / "test_pipeline.py").write_text("def test_dummy(): pass")
    return d


class TestCheckPipelineDir:
    def test_all_required_files_present(self, pipeline_dir: Path) -> None:
        """Happy path: all six required files present."""
        errors = check_pipeline_dir(pipeline_dir)
        assert errors == []

    def test_missing_required_file(self, tmp_path: Path) -> None:
        """Missing fixtures.py should raise error."""
        d = tmp_path / "bad_pipeline"
        d.mkdir()
        (d / "__init__.py").write_text("")
        (d / "config.py").write_text(
            "from validation import ValidatedStruct, Expectations\n"
            "class X(ValidatedStruct):\n pass\n"
            "class XExp(Expectations):\n pass\n"
        )
        (d / "pipeline.py").write_text("from poorbricks import pipeline")
        (d / "transform.py").write_text("")
        # Missing fixtures.py
        (d / "test_pipeline.py").write_text("")

        errors = check_pipeline_dir(d)
        assert len(errors) == 1
        assert "missing fixtures.py" in errors[0].format()

    def test_no_poorbricks_import(self, tmp_path: Path) -> None:
        """pipeline.py without poorbricks import should error."""
        d = tmp_path / "legacy_pipeline"
        d.mkdir()
        (d / "__init__.py").write_text("")
        (d / "config.py").write_text(
            "from validation import ValidatedStruct, Expectations\n"
            "class X(ValidatedStruct):\n pass\n"
            "class XExp(Expectations):\n pass\n"
        )
        (d / "pipeline.py").write_text("# no poorbricks import\n")
        (d / "transform.py").write_text("")
        (d / "fixtures.py").write_text("")
        (d / "test_pipeline.py").write_text("")

        errors = check_pipeline_dir(d)
        assert any("does not import from poorbricks" in e.format() for e in errors)

    def test_no_scenario_in_fixtures(self, tmp_path: Path) -> None:
        """fixtures.py without @scenario should error."""
        d = tmp_path / "no_scenario"
        d.mkdir()
        (d / "__init__.py").write_text("")
        (d / "config.py").write_text(
            "from validation import ValidatedStruct, Expectations\n"
            "class X(ValidatedStruct):\n pass\n"
            "class XExp(Expectations):\n pass\n"
        )
        (d / "pipeline.py").write_text("from poorbricks import pipeline")
        (d / "transform.py").write_text("")
        (d / "fixtures.py").write_text("# no @scenario\n")
        (d / "test_pipeline.py").write_text("")

        errors = check_pipeline_dir(d)
        assert any("@scenario" in e.format() for e in errors)

    def test_no_expectations_subclass(self, tmp_path: Path) -> None:
        """config.py without Expectations subclass should error."""
        d = tmp_path / "no_expectations"
        d.mkdir()
        (d / "__init__.py").write_text("")
        (d / "config.py").write_text(
            "from validation import ValidatedStruct\nclass X(ValidatedStruct):\n pass\n"
        )
        (d / "pipeline.py").write_text("from poorbricks import pipeline")
        (d / "transform.py").write_text("")
        (d / "fixtures.py").write_text(
            "from poorbricks import scenario\n@scenario('x')\ndef x(): pass"
        )
        (d / "test_pipeline.py").write_text("")

        errors = check_pipeline_dir(d)
        assert any("missing a subclass of Expectations" in e.format() for e in errors)

    def test_silver_missing_unique_keys(self, tmp_path: Path) -> None:
        """Silver level without UNIQUE_KEYS should error."""
        d = tmp_path / "silver_no_keys"
        d.mkdir()
        (d / "__init__.py").write_text("")
        (d / "config.py").write_text(
            "from validation import ValidatedStruct, Expectations\n"
            "class X(ValidatedStruct):\n pass\n"
            "class XExp(Expectations):\n pass\n"
        )
        (d / "pipeline.py").write_text(
            "from poorbricks import pipeline\n"
            "@pipeline(name='x', model=X, level='silver', comment='test')\n"
            "def x(inputs): pass\n"
        )
        (d / "transform.py").write_text("")
        (d / "fixtures.py").write_text(
            "from poorbricks import scenario\n@scenario('x')\ndef x(): pass"
        )
        (d / "test_pipeline.py").write_text("")

        errors = check_pipeline_dir(d)
        assert any("UNIQUE_KEYS" in e.format() for e in errors)

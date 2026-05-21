"""Smoke tests for the framework skeleton.

These tests exercise Inputs, @pipeline, @scenario, and the fixtures-mode
runner against a self-contained dummy pipeline declared inside this file.
No real pipeline migration required — this is the framework's own contract.
"""

from __future__ import annotations

from typing import Annotated, Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from utils.dataframes import create_dataframe
from validation import NotNullRule, ValidatedStruct, ValidationRule

from . import Inputs, TableSource, pipeline, scenario
from .registry import _pipelines, _registry_key, _scenarios
from .runner import run


class _Patient(ValidatedStruct):
    """Synthetic upstream model for framework tests."""

    id: str
    name: str

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [NotNullRule(column="id"), NotNullRule(column="name")]


class _Greeting(ValidatedStruct):
    """Synthetic output model — what the dummy pipeline produces."""

    id: str
    greeting: str

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [NotNullRule(column="id"), NotNullRule(column="greeting")]


_FW_TEST_TABLE = "_fw_test_dummy_pipeline"
# Registry keys pipelines by (target_storage, table_name).
# This dummy pipeline registers with the default storage="delta",
# so its composite key is "delta:_fw_test_dummy_pipeline".
_FW_TEST_KEY = _registry_key(_FW_TEST_TABLE, "delta")


class _DummyInputs(Inputs):
    patients: Annotated[DataFrame, TableSource("_fw_test_patients", _Patient)]


@pipeline(
    name=_FW_TEST_TABLE,
    model=_Greeting,
    level="silver",
    comment="Synthetic pipeline used by framework smoke tests.",
)
def _fw_test_dummy_pipeline(inputs: _DummyInputs) -> DataFrame:
    return create_dataframe(
        inputs.patients.select(
            f.col("id"),
            f.concat(f.lit("hello "), f.col("name")).alias("greeting"),
        ),
        _Greeting.to_struct(),
    )


@scenario("alice")
def _alice_scenario() -> _DummyInputs:
    return _DummyInputs.from_rows({"patients": [{"id": "p1", "name": "alice"}]})


@scenario("bob")
def _bob_scenario() -> _DummyInputs:
    return _DummyInputs.from_rows({"patients": [{"id": "p2", "name": "bob"}]})


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestInputsSpec:
    def test_sources_extracted_from_annotations(self) -> None:
        sources = _DummyInputs.sources()
        assert set(sources) == {"patients"}
        assert isinstance(sources["patients"], TableSource)
        assert sources["patients"].table_name == "_fw_test_patients"
        assert sources["patients"].model is _Patient

    def test_sources_cached_per_subclass(self) -> None:
        first = _DummyInputs.sources()
        second = _DummyInputs.sources()
        assert first == second

    @pytest.mark.spark
    def test_from_rows_builds_dataframes_with_declared_schemas(
        self, spark: SparkSession
    ) -> None:
        inputs = _DummyInputs.from_rows({"patients": [{"id": "x", "name": "y"}]})
        assert inputs.patients.schema == _Patient.to_struct()
        rows = inputs.patients.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == "x"

    @pytest.mark.spark
    def test_from_dataframes_rejects_missing_keys(self, spark: SparkSession) -> None:
        with pytest.raises(ValueError, match="missing dataframes"):
            _DummyInputs.from_dataframes({})

    @pytest.mark.spark
    def test_from_dataframes_rejects_extra_keys(self, spark: SparkSession) -> None:
        df = create_dataframe([{"id": "p1", "name": "x"}], _Patient.to_struct())
        with pytest.raises(ValueError, match="unexpected dataframes"):
            _DummyInputs.from_dataframes({"patients": df, "stranger": df})


class TestPipelineDecorator:
    def test_pipeline_registered(self) -> None:
        assert _FW_TEST_KEY in _pipelines
        meta = _pipelines[_FW_TEST_KEY]
        assert meta.inputs_cls is _DummyInputs
        assert meta.model is _Greeting
        assert meta.level == "silver"

    def test_pipeline_rejects_invalid_level(self) -> None:
        with pytest.raises(ValueError, match="bronze"):
            pipeline(
                name="bad",
                model=_Greeting,
                level="platinum",
                comment="x",
            )

    def test_pipeline_rejects_function_without_inputs_param(self) -> None:
        with pytest.raises(TypeError, match="Inputs"):

            @pipeline(name="bad2", model=_Greeting, level="silver", comment="x")
            def _no_param() -> DataFrame:  # type: ignore[empty-body]
                ...


class TestScenarioRegistry:
    def test_scenarios_registered_under_pipeline_key(self) -> None:
        # The scenarios are declared in this module; the @scenario decorator
        # strips ".fixtures" but this module isn't actually named "fixtures",
        # so the key is the full module path minus the source.pipelines prefix.
        # That is intentional: in a real pipeline the scenarios live in
        # source.pipelines.<key>.fixtures, which strips cleanly. Here we just
        # confirm both scenarios landed under the same key.
        keys_with_alice = [k for k, v in _scenarios.items() if "alice" in v]
        keys_with_bob = [k for k, v in _scenarios.items() if "bob" in v]
        assert len(keys_with_alice) == 1
        assert keys_with_alice == keys_with_bob


class TestRunner:
    @pytest.mark.spark
    def test_fixtures_mode_unions_all_scenarios(self, spark: SparkSession) -> None:
        meta = _pipelines[_FW_TEST_KEY]
        from .runner import _build_fixtures_inputs

        scenario_module_keys = [
            k for k, v in _scenarios.items() if "alice" in v and "bob" in v
        ]
        assert len(scenario_module_keys) == 1
        inputs = _build_fixtures_inputs(
            scenario_module_keys[0], meta.inputs_cls, scenario_name=None
        )
        df = meta.original_fn(inputs)
        rows = sorted(r["greeting"] for r in df.collect())
        assert rows == ["hello alice", "hello bob"]

    @pytest.mark.spark
    def test_scenario_mode_runs_single_scenario(self, spark: SparkSession) -> None:
        meta = _pipelines[_FW_TEST_KEY]
        from .runner import _build_fixtures_inputs

        scenario_module_keys = [k for k, v in _scenarios.items() if "alice" in v]
        inputs = _build_fixtures_inputs(
            scenario_module_keys[0], meta.inputs_cls, scenario_name="alice"
        )
        df = meta.original_fn(inputs)
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["greeting"] == "hello alice"

    @pytest.mark.spark
    def test_run_via_real_pipeline_module(self, spark: SparkSession) -> None:
        """End-to-end: importlib path → registry lookup → execute.

        Validates that ``runner.run("fw_smoke")``-style invocation works.
        Monkey-patches the import path to point at this test module so we
        don't need a real migrated pipeline.
        """
        import sys

        alias = "tables.fw_smoke.pipeline"
        sys.modules[alias] = sys.modules[__name__]
        from .registry import _scenarios as scen_reg

        scen_reg["fw_smoke"] = {
            "alice": _alice_scenario,
            "bob": _bob_scenario,
        }
        meta = _pipelines[_FW_TEST_KEY]
        original_module = meta.module
        meta.module = alias
        try:
            result = run("fw_smoke", mode="fixtures", skip_checks=True)
            assert result.df is not None
            rows = sorted(r["greeting"] for r in result.df.collect())
            assert rows == ["hello alice", "hello bob"]
        finally:
            meta.module = original_module
            del sys.modules[alias]
            scen_reg.pop("fw_smoke", None)

    @pytest.mark.spark
    def test_fault_mode_breaks_inputs(self, spark: SparkSession) -> None:
        """Smoke-test fault mode: empty_inputs should produce zero output rows."""
        import sys

        alias = "tables.fw_fault_smoke.pipeline"
        sys.modules[alias] = sys.modules[__name__]
        from .registry import _scenarios as scen_reg

        scen_reg["fw_fault_smoke"] = {"alice": _alice_scenario}
        meta = _pipelines[_FW_TEST_KEY]
        original_module = meta.module
        meta.module = alias
        try:
            result = run(
                "fw_fault_smoke",
                mode="fault",
                fault_name="empty_inputs",
                skip_checks=True,
            )
            assert result.df is not None
            assert result.df.count() == 0
        finally:
            meta.module = original_module
            del sys.modules[alias]
            scen_reg.pop("fw_fault_smoke", None)

    @pytest.mark.spark
    def test_unknown_mode_raises(self, spark: SparkSession) -> None:
        with pytest.raises(ValueError, match="Unknown mode"):
            run("fw_smoke_doesnt_matter", mode="bogus")

    def test_result_rows_populated(self) -> None:
        """Verify that result.rows is populated after compute."""
        import sys

        alias = "tables.fw_rows_test.pipeline"
        sys.modules[alias] = sys.modules[__name__]
        from .registry import _scenarios as scen_reg

        scen_reg["fw_rows_test"] = {"alice": _alice_scenario}
        meta = _pipelines[_FW_TEST_KEY]
        original_module = meta.module
        meta.module = alias
        try:
            result = run("fw_rows_test", mode="fixtures", skip_checks=True)
            assert result.df is not None
            assert result.rows is not None
            assert result.rows > 0
        finally:
            meta.module = original_module
            del sys.modules[alias]
            scen_reg.pop("fw_rows_test", None)

    def test_validate_mode_no_compute(self) -> None:
        """Validate mode does not compute, returns no data."""
        import sys

        alias = "tables.fw_validate_test.pipeline"
        sys.modules[alias] = sys.modules[__name__]
        from .registry import _scenarios as scen_reg

        scen_reg["fw_validate_test"] = {"alice": _alice_scenario}
        meta = _pipelines[_FW_TEST_KEY]
        original_module = meta.module
        meta.module = alias
        try:
            result = run("fw_validate_test", mode="validate", skip_checks=True)
            assert result.df is None
            assert result.rows is None
        finally:
            meta.module = original_module
            del sys.modules[alias]
            scen_reg.pop("fw_validate_test", None)

    def test_skip_checks_bypasses_validation(self) -> None:
        """skip_checks=True bypasses arch and contract checks."""
        import sys

        alias = "tables.fw_skip_checks.pipeline"
        sys.modules[alias] = sys.modules[__name__]
        from .registry import _scenarios as scen_reg

        scen_reg["fw_skip_checks"] = {"alice": _alice_scenario}
        meta = _pipelines[_FW_TEST_KEY]
        original_module = meta.module
        meta.module = alias
        try:
            result = run("fw_skip_checks", mode="fixtures", skip_checks=True)
            assert result.df is not None
            assert result.rows is not None
        finally:
            meta.module = original_module
            del sys.modules[alias]
            scen_reg.pop("fw_skip_checks", None)


class TestTableSourceProductionResolution:
    """In production a TableSource reads its upstream from the Postgres warehouse.

    The legacy ``spark.read.table()`` catalog read only works in fixtures mode;
    in production an in-bundle TableSource resolves against its registered
    producer (schema from the local model, ``level`` from the registry), and a
    TableSource with no in-bundle producer falls back to the published contract.
    """

    def test_in_bundle_producer_reads_postgres_with_producer_level(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from . import runner

        captured: dict[str, Any] = {}

        def fake_read_postgres_table(
            spark: Any, level: str, table_name: str, schema_json: Any
        ) -> str:
            captured.update(level=level, table_name=table_name, schema_json=schema_json)
            return "PG_DF"

        class _FakeMeta:
            level = "bronze"
            target_storage = "postgres"

        monkeypatch.setattr(runner, "_read_postgres_table", fake_read_postgres_table)
        monkeypatch.setattr(runner, "_find_local_producer", lambda _name: _FakeMeta())

        spec = TableSource("_fw_test_patients", _Patient)
        result = runner._resolve_production_input(
            spark=None, spec=spec, mongo_uri=None, cache={}
        )

        assert result == "PG_DF"
        assert captured["level"] == "bronze"
        assert captured["table_name"] == "_fw_test_patients"
        assert captured["schema_json"] == _Patient.to_struct().jsonValue()

    def test_cross_repo_falls_back_to_published_contract(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from . import runner

        calls: list[str] = []

        def fake_read_contract_source(spark: Any, table_name: str) -> str:
            calls.append(table_name)
            return "CONTRACT_DF"

        monkeypatch.setattr(runner, "_find_local_producer", lambda _name: None)
        monkeypatch.setattr(runner, "_read_contract_source", fake_read_contract_source)

        spec = TableSource("_fw_test_patients", _Patient)
        result = runner._resolve_production_input(
            spark=None, spec=spec, mongo_uri=None, cache={}
        )

        assert result == "CONTRACT_DF"
        assert calls == ["_fw_test_patients"]

    def test_non_postgres_producer_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from . import runner

        class _DeltaMeta:
            level = "bronze"
            target_storage = "delta"

        monkeypatch.setattr(runner, "_find_local_producer", lambda _name: _DeltaMeta())

        spec = TableSource("_fw_test_patients", _Patient)
        with pytest.raises(ValueError, match="only 'postgres'"):
            runner._resolve_production_input(
                spark=None, spec=spec, mongo_uri=None, cache={}
            )

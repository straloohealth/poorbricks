"""Unit tests for ``_check_pipeline_contracts`` in-bundle source resolution,
plus ``verify_db`` — running a MongoSource pipeline against a DB-derived
synthetic contract.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Annotated, Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from poorbricks import Inputs
from poorbricks.inputs import ContractSource, MongoSource, TableSource
from poorbricks.verify import _check_pipeline_contracts, _verify_db_pipeline
from validation import Expectations, NotNullRule, ValidatedStruct, ValidationRule


class _UserModel(ValidatedStruct):
    user_id: str
    name: str | None


class _DriftedModel(ValidatedStruct):
    user_id: str
    surprise: int | None


class _TableConsumerInputs(Inputs):
    up: Annotated[DataFrame, TableSource("users_bronze", _UserModel)]


class _DriftConsumerInputs(Inputs):
    up: Annotated[DataFrame, TableSource("users_bronze", _DriftedModel)]


class _ContractConsumerInputs(Inputs):
    up: Annotated[DataFrame, ContractSource("users_bronze")]


def _no_contracts(table_name: str) -> dict[str, Any]:
    raise KeyError(table_name)


def _meta(inputs_cls: type[Inputs]) -> Any:
    return SimpleNamespace(inputs_cls=inputs_cls)


def _producer(model: type) -> Any:
    return SimpleNamespace(model=model)


def test_in_bundle_table_source_validates_against_local_producer() -> None:
    """A TableSource on an in-bundle producer ignores the published contract."""
    errors = _check_pipeline_contracts(
        "postgres:consumer",
        _meta(_TableConsumerInputs),
        _no_contracts,  # store has nothing — must not matter
        {"users_bronze": _producer(_UserModel)},
    )
    assert errors == []


def test_in_bundle_table_source_drift_against_local_producer() -> None:
    """A consumer model disagreeing with the in-bundle producer is still drift."""
    errors = _check_pipeline_contracts(
        "postgres:consumer",
        _meta(_DriftConsumerInputs),
        _no_contracts,
        {"users_bronze": _producer(_UserModel)},
    )
    assert len(errors) == 1
    assert errors[0].reason == "schema_drift"


def test_in_bundle_contract_source_skips_store() -> None:
    """A ContractSource on an in-bundle producer needs no published contract."""
    errors = _check_pipeline_contracts(
        "postgres:consumer",
        _meta(_ContractConsumerInputs),
        _no_contracts,
        {"users_bronze": _producer(_UserModel)},
    )
    assert errors == []


def test_cross_repo_table_source_checks_published_contract() -> None:
    """An upstream not in the bundle is validated against the published contract."""

    def fetcher(table_name: str) -> dict[str, Any]:
        return {"schema_json": _UserModel.to_struct().jsonValue()}

    errors = _check_pipeline_contracts(
        "postgres:consumer", _meta(_TableConsumerInputs), fetcher, {}
    )
    assert errors == []


def test_cross_repo_missing_contract_fails() -> None:
    """An upstream not in the bundle with no published contract is an error."""
    errors = _check_pipeline_contracts(
        "postgres:consumer", _meta(_ContractConsumerInputs), _no_contracts, {}
    )
    assert len(errors) == 1
    assert errors[0].reason == "missing_contract"


# ---------------------------------------------------------------------------
# verify_db — run a MongoSource pipeline against a DB-derived synthetic contract
# ---------------------------------------------------------------------------


class _WidgetBronze(ValidatedStruct):
    """Bronze contract that *requires* ``required_note``."""

    mongo_id: str
    name: str | None
    required_note: str | None

    @classmethod
    def rules(cls) -> list[ValidationRule]:
        return [NotNullRule(column="required_note")]


class _WidgetInputs(Inputs):
    upstream: Annotated[
        DataFrame,
        MongoSource(db="shop", collection="widgets", schema=_WidgetBronze.to_struct()),
    ]


def _widget_compute(inputs: _WidgetInputs) -> DataFrame:
    """Bronze pass-through transform."""
    from utils.dataframes import create_dataframe

    return create_dataframe(inputs.upstream, _WidgetBronze.to_struct())


def _widget_meta() -> Any:
    return SimpleNamespace(
        inputs_cls=_WidgetInputs,
        original_fn=_widget_compute,
        model=_WidgetBronze,
        module="poorbricks.test_verify",
    )


def _stub_fetcher(
    real_docs: list[dict[str, Any]],
) -> Callable[[str, str, int], dict[str, Any]]:
    """Build a /v1/db-contract stub from documents a collection would hold."""
    from utils.schema_infer import infer
    from utils.synth_data import generate

    result = infer(real_docs)
    contract = {
        "schema_json": result.struct.jsonValue(),
        "example_rows": generate(result.struct, result.profile, n=15),
    }
    return lambda _db, _collection, _n: contract


@pytest.mark.spark
def test_verify_db_catches_contract_field_absent_from_collection(
    spark: SparkSession,
) -> None:
    """watson_tasks-class regression: the contract requires ``required_note``
    but the real collection never provides it — verify --mode db must fail."""
    real_docs = [{"_id": f"{i:024x}", "name": f"w{i}"} for i in range(15)]
    errors = _verify_db_pipeline(
        "postgres:widget",
        _widget_meta(),
        {"upstream": _WidgetInputs.sources()["upstream"]},
        _stub_fetcher(real_docs),
        sample_size=15,
        spark=spark,
    )
    assert errors, "verify_db must flag a required field missing from real data"
    assert any(e.category in {"rule", "run_error"} for e in errors)


@pytest.mark.spark
def test_verify_db_passes_when_collection_matches_contract(
    spark: SparkSession,
) -> None:
    """Real data carries ``requiredNote`` (camelCase) — the production
    document-prep path renames it to ``required_note`` and verify passes."""
    real_docs = [
        {"_id": f"{i:024x}", "name": f"w{i}", "requiredNote": "ok note"}
        for i in range(15)
    ]
    errors = _verify_db_pipeline(
        "postgres:widget",
        _widget_meta(),
        {"upstream": _WidgetInputs.sources()["upstream"]},
        _stub_fetcher(real_docs),
        sample_size=15,
        spark=spark,
    )
    assert errors == []


@pytest.mark.spark
def test_verify_db_does_not_enforce_production_expectations(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """db mode feeds the pipeline synthetic rows derived from a *dev*
    collection. ``Expectations`` (MIN_ROWS / ENUM_VALUES / FRESH_COLUMN / …)
    are production-health monitors and must not gate verify --mode db — they
    would report false failures on seed data. Only schema + ``ValidationRule``s
    (model.verify) decide. Regression guard: if the Expectations check is ever
    re-added to ``_verify_db_pipeline``, ``check`` runs and the test fails."""

    class _NeverRunHere(Expectations):
        @classmethod
        def check(cls, df: DataFrame, *, enforce_min_rows: bool = True) -> list[str]:
            raise AssertionError("Expectations.check must not run in verify --mode db")

    monkeypatch.setattr(
        "poorbricks.verify._find_expectations_for", lambda _meta: _NeverRunHere
    )
    real_docs = [
        {"_id": f"{i:024x}", "name": f"w{i}", "requiredNote": "ok note"}
        for i in range(15)
    ]
    errors = _verify_db_pipeline(
        "postgres:widget",
        _widget_meta(),
        {"upstream": _WidgetInputs.sources()["upstream"]},
        _stub_fetcher(real_docs),
        sample_size=15,
        spark=spark,
    )
    assert errors == []


@pytest.mark.spark
def test_verify_db_skips_empty_collection(spark: SparkSession) -> None:
    """An empty collection yields no DB-derived contract — db mode has no data
    to exercise the pipeline, so it skips with a warning instead of failing."""

    def _empty_fetcher(db: str, collection: str, n: int) -> dict[str, Any]:
        raise KeyError(f"{db}.{collection}")

    errors = _verify_db_pipeline(
        "postgres:widget",
        _widget_meta(),
        {"upstream": _WidgetInputs.sources()["upstream"]},
        _empty_fetcher,
        sample_size=15,
        spark=spark,
    )
    assert errors == []

"""Unit tests for ``_check_pipeline_contracts`` in-bundle source resolution."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Annotated, Any

from pyspark.sql import DataFrame

from poorbricks import Inputs
from poorbricks.inputs import ContractSource, TableSource
from poorbricks.verify import _check_pipeline_contracts
from validation import ValidatedStruct


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

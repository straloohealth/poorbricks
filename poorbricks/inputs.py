"""Typed upstream declarations.

A pipeline declares its upstreams as `Annotated[DataFrame, TableSource(...)]`
attributes on an `Inputs` subclass. The framework reads those annotations to
decide what to do for each verify mode (resolve from Mongo, fixtures, contracts
store, or per-input overrides).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, TypeVar, get_type_hints

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

    from validation import ValidatedStruct


@dataclass(frozen=True)
class TableSource:
    """Declares a registered upstream table source."""

    table_name: str
    model: type[ValidatedStruct]


@dataclass(frozen=True)
class MongoSource:
    """Declares a MongoDB upstream.

    Connection is resolved from MONGO_URI in the environment (or .env).

    ``nullable_columns`` lists columns whose source values may be null/missing
    even though the contract declares them non-nullable. The *read* relaxes
    those to nullable so one bad source document never aborts the whole read;
    the pipeline then imputes them via ``Expectations.IMPUTE_DEFAULTS`` (which
    also records a non-critical warning), keeping the contract non-nullable.
    """

    db: str
    collection: str
    schema: StructType
    nullable_columns: tuple[str, ...] = ()

    @property
    def read_schema(self) -> StructType:
        """The schema used to read Mongo: ``schema`` with ``nullable_columns``
        relaxed to nullable. Identical to ``schema`` when none are declared."""
        if not self.nullable_columns:
            return self.schema
        from pyspark.sql.types import StructField, StructType

        relax = set(self.nullable_columns)
        return StructType(
            [
                StructField(f.name, f.dataType, nullable=True) if f.name in relax else f
                for f in self.schema.fields
            ]
        )


@dataclass(frozen=True)
class ContractSource:
    """Declares an upstream whose schema comes from the MongoDB contracts store.

    Silver/gold pipelines declare inputs this way instead of importing bronze
    model classes directly — enabling cross-repo consumption without code coupling.
    The schema and example rows are resolved at runtime from the contracts collection.
    """

    table_name: str


@dataclass(frozen=True)
class PostgresTableSource:
    """Declares a Postgres-target upstream."""

    schema: str
    table: str


SourceSpec = TableSource | MongoSource | ContractSource | PostgresTableSource

_InputsT = TypeVar("_InputsT", bound="Inputs")


class Inputs:
    """Base for typed upstream declarations.

    Subclass shape::

        from typing import Annotated
        from pyspark.sql import DataFrame
        from  import Inputs, TableSource

        class MyInputs(Inputs):
            patients: Annotated[DataFrame, TableSource(PATIENTS_TABLE_NAME, Patients)]
            criteria: Annotated[DataFrame, TableSource(CRITERIA_TABLE_NAME, Criteria)]

    Resolved instance attributes are DataFrames. Use ``MyInputs.sources()`` to
    introspect the declarations or ``MyInputs.from_dataframes({...})`` /
    ``MyInputs.from_rows({...})`` to build a concrete instance.
    """

    # Holds the resolved sources plus a "__cls__" marker entry, so the value
    # type is Any rather than SourceSpec.
    _sources_cache: ClassVar[dict[str, Any] | None] = None

    @classmethod
    def sources(cls) -> dict[str, SourceSpec]:
        """Return ``{attr_name: SourceSpec}`` extracted from class annotations.

        Cached per-subclass after first call.
        """
        if cls._sources_cache is not None and cls._sources_cache.get("__cls__") is cls:
            cache = dict(cls._sources_cache)
            cache.pop("__cls__", None)
            return cache

        result: dict[str, SourceSpec] = {}
        hints = get_type_hints(cls, include_extras=True)
        for attr_name, hint in hints.items():
            metadata = getattr(hint, "__metadata__", ())
            for meta in metadata:
                if isinstance(
                    meta,
                    TableSource | MongoSource | ContractSource | PostgresTableSource,
                ):
                    result[attr_name] = meta
                    break

        # Stash with a class marker so a subclass doesn't accidentally read the
        # parent's cache.
        cls._sources_cache = {"__cls__": cls, **result}
        return result

    @classmethod
    def from_dataframes(
        cls: type[_InputsT], dataframes: dict[str, DataFrame]
    ) -> _InputsT:
        """Build an instance with each declared input set to the provided DataFrame."""
        sources = cls.sources()
        missing = set(sources) - set(dataframes)
        extra = set(dataframes) - set(sources)
        if missing:
            raise ValueError(
                f"{cls.__name__}: missing dataframes for {sorted(missing)}"
            )
        if extra:
            raise ValueError(
                f"{cls.__name__}: unexpected dataframes for {sorted(extra)}"
            )
        obj = cls.__new__(cls)
        for name, df in dataframes.items():
            setattr(obj, name, df)
        return obj

    @classmethod
    def from_rows(
        cls: type[_InputsT], rows: dict[str, list[dict[str, Any]]]
    ) -> _InputsT:
        """Build an instance from raw row dicts. Used by fixtures.

        Each list[dict] is converted to a DataFrame using the source's declared
        schema (TableSource.model.to_struct(), MongoSource.schema, or ContractSource
        schema fetched from MongoDB contracts).
        """
        from pyspark.sql.types import StructType

        from utils.contracts import fetch_contract
        from utils.dataframes import create_dataframe

        sources = cls.sources()
        dataframes: dict[str, DataFrame] = {}
        for name, spec in sources.items():
            spec_rows = rows.get(name, [])
            if isinstance(spec, MongoSource):
                schema = spec.schema
            elif isinstance(spec, ContractSource):
                contract = fetch_contract(spec.table_name)
                schema = StructType.fromJson(contract["schema_json"])
            elif isinstance(spec, PostgresTableSource):
                raise ValueError(
                    f"PostgresTableSource {spec.table!r} is not supported "
                    f"in fixtures/scenario mode; it is only for legacy gold passthroughs."
                )
            else:
                schema = spec.model.to_struct()
            dataframes[name] = create_dataframe(spec_rows, schema)
        return cls.from_dataframes(dataframes)


__all__ = [
    "Annotated",
    "Inputs",
    "MongoSource",
    "PostgresTableSource",
    "SourceSpec",
    "TableSource",
]

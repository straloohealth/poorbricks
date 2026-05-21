"""Contract and expectations verification for a table-repo.

Three modes:

* ``verify_local`` — schema-only check against the contracts store. Fast, no
  Spark. Detects: missing contracts; for ``TableSource`` inputs, schema
  drift between the local model and the published contract.

* ``verify_ci`` — full pipeline execution against fixtures (or production
  data), then runs ``ValidatedStruct`` rules and ``Expectations`` checks.
  Optionally exports a profile JSON per pipeline. Does not write.

* ``verify_mongo`` — fast real-data check (no Spark). Connects to MongoDB via
  ``MONGO_URI`` env var, samples 100 oldest + 100 newest docs per collection,
  and checks that every field declared in each ``MongoSource`` schema is
  present in at least one sampled document.

Used by the ``poorbricks-verify`` CLI in ``[tool.poetry.scripts]``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from poorbricks.arch import ArchError, check_architecture
from poorbricks.discovery import discover_all_pipelines
from poorbricks.inputs import ContractSource, MongoSource, TableSource
from poorbricks.registry import PipelineMeta, all_pipelines

ContractFetcher = Callable[[str], dict[str, Any]]


@dataclass
class ContractError:
    """A local-mode failure: missing contract or schema mismatch."""

    pipeline_key: str
    input_name: str
    upstream: str
    reason: str  # "missing_contract" | "schema_drift"
    details: list[str] = field(default_factory=list)

    def format(self) -> str:
        head = f"✗ {self.pipeline_key} [{self.input_name} -> {self.upstream}]: {self.reason}"
        if self.details:
            return head + "\n    " + "\n    ".join(self.details)
        return head


@dataclass
class VerificationError:
    """A CI-mode failure: rule, expectation, or drift violation."""

    pipeline_key: str
    category: str  # "rule" | "expectation" | "drift" | "run_error"
    message: str

    def format(self) -> str:
        return f"✗ {self.pipeline_key} [{self.category}]: {self.message}"


_DEFAULT_CONTRACT_URL = (
    "https://airflow-poorbricks-server-ingress.stingray-ordinal.ts.net"
)


def _default_fetcher() -> ContractFetcher:
    from utils.contracts import fetch_contract

    return fetch_contract


def _http_fetcher(base_url: str) -> ContractFetcher:
    """Fetch contracts from the poorbricks server HTTP endpoint.

    Raises KeyError when the server returns 404 (contract not found).
    """
    import requests

    def fetch(table_name: str) -> dict[str, Any]:
        url = f"{base_url.rstrip('/')}/v1/contracts/{table_name}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            raise KeyError(table_name)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    return fetch


def _compare_schemas(
    local_struct: Any, published_schema_json: dict[str, Any]
) -> list[str]:
    """Return a list of human-readable diffs between local and published schemas.

    Empty list means the schemas are compatible. Uses field name + type
    comparison: additions, removals, type changes.
    """
    from pyspark.sql.types import StructType

    published = StructType.fromJson(published_schema_json)
    local_types = {f.name: f.dataType.simpleString() for f in local_struct.fields}
    published_types = {f.name: f.dataType.simpleString() for f in published.fields}

    diffs: list[str] = []
    for name in sorted(set(local_types) - set(published_types)):
        diffs.append(f"field {name!r} declared locally is not in published contract")
    for name in sorted(set(published_types) - set(local_types)):
        diffs.append(f"field {name!r} in published contract is not declared locally")
    for name in sorted(set(local_types) & set(published_types)):
        if local_types[name] != published_types[name]:
            diffs.append(
                f"field {name!r} type mismatch: local={local_types[name]} "
                f"published={published_types[name]}"
            )
    return diffs


def _check_pipeline_contracts(
    key: str,
    meta: PipelineMeta,
    fetcher: ContractFetcher,
    local_tables: dict[str, PipelineMeta],
) -> list[ContractError]:
    """Check one pipeline's upstreams.

    When an upstream is produced by another pipeline in the same upload bundle
    (``local_tables``), it is resolved against that local producer's declared
    output schema — never the published contract. The upload refreshes that
    contract atomically, so a stale or absent published copy is irrelevant and
    must not block a repo that publishes a source table alongside its consumer.
    """
    errors: list[ContractError] = []
    for input_name, spec in meta.inputs_cls.sources().items():
        if isinstance(spec, ContractSource):
            if spec.table_name in local_tables:
                continue
            try:
                fetcher(spec.table_name)
            except KeyError:
                errors.append(
                    ContractError(
                        pipeline_key=key,
                        input_name=input_name,
                        upstream=spec.table_name,
                        reason="missing_contract",
                    )
                )
        elif isinstance(spec, TableSource):
            producer = local_tables.get(spec.table_name)
            if producer is not None:
                published_schema_json = producer.model.to_struct().jsonValue()  # type: ignore[attr-defined]
            else:
                try:
                    published_schema_json = fetcher(spec.table_name)["schema_json"]
                except KeyError:
                    errors.append(
                        ContractError(
                            pipeline_key=key,
                            input_name=input_name,
                            upstream=spec.table_name,
                            reason="missing_contract",
                        )
                    )
                    continue
            diffs = _compare_schemas(spec.model.to_struct(), published_schema_json)
            if diffs:
                errors.append(
                    ContractError(
                        pipeline_key=key,
                        input_name=input_name,
                        upstream=spec.table_name,
                        reason="schema_drift",
                        details=diffs,
                    )
                )
    return errors


@dataclass
class MongoCheckError:
    """A mongo-mode failure: schema field not present in any sampled document."""

    pipeline_key: str
    input_name: str
    db: str
    collection: str
    missing_fields: list[str]
    extra_fields: list[str]

    def format(self) -> str:
        parts = [
            f"✗ {self.pipeline_key} [{self.input_name} → {self.db}.{self.collection}]"
        ]
        if self.missing_fields:
            parts.append(
                f"  MISSING in docs (schema declares but never seen): {self.missing_fields}"
            )
        if self.extra_fields:
            parts.append(f"  EXTRA in docs (not in schema): {self.extra_fields}")
        return "\n".join(parts)


def _public_uri(mongo_uri: str) -> str:
    """Strip the Atlas private-peering suffix (-pri) so the public endpoint is used."""
    return mongo_uri.replace("-pri.", ".")


def _sample_mongo_collection(
    mongo_uri: str, db: str, collection: str, sample_size: int = 100
) -> list[dict[str, Any]]:
    """Return up to sample_size oldest + sample_size newest docs, deduplicated."""
    import pymongo

    client: pymongo.MongoClient[dict[str, Any]] = pymongo.MongoClient(
        _public_uri(mongo_uri)
    )
    coll = client[db][collection]
    oldest = list(coll.find({}, limit=sample_size).sort("_id", pymongo.ASCENDING))
    newest = list(coll.find({}, limit=sample_size).sort("_id", pymongo.DESCENDING))
    seen: set[str] = set()
    combined: list[dict[str, Any]] = []
    for doc in oldest + newest:
        key = str(doc.get("_id", id(doc)))
        if key not in seen:
            seen.add(key)
            combined.append(doc)
    return combined


def verify_mongo(
    tables_root: Path | None = None,
    mongo_uri: str | None = None,
    sample_size: int = 100,
) -> list[MongoCheckError]:
    """Fast real-data check. No Spark. Requires live MongoDB via MONGO_URI.

    For each ``MongoSource`` in registered pipelines, fetches up to
    ``sample_size`` oldest + ``sample_size`` newest documents and compares
    the union of field names against the declared schema fields.
    """
    import os

    uri = mongo_uri or os.getenv("MONGO_URI")
    if not uri:
        raise ValueError(
            "MONGO_URI must be set (env var or .env file) to run --mode mongo."
        )

    discover_all_pipelines(tables_root)
    errors: list[MongoCheckError] = []

    for key, meta in all_pipelines().items():
        for input_name, spec in meta.inputs_cls.sources().items():
            if not isinstance(spec, MongoSource):
                continue
            schema_fields = {f.name for f in spec.schema.fields}
            # Strip the framework's synthetic mongo_id alias: _id is not a user field
            schema_fields.discard("mongo_id")

            try:
                docs = _sample_mongo_collection(
                    uri, spec.db, spec.collection, sample_size
                )
            except Exception as exc:
                errors.append(
                    MongoCheckError(
                        pipeline_key=key,
                        input_name=input_name,
                        db=spec.db,
                        collection=spec.collection,
                        missing_fields=[f"<connection error: {exc}>"],
                        extra_fields=[],
                    )
                )
                continue

            if not docs:
                print(
                    f"  ⚠  {key}: {spec.db}.{spec.collection} is empty — skipping field check"
                )
                continue

            from utils.mongo import _camel_to_snake

            raw_fields: set[str] = set()
            for doc in docs:
                raw_fields.update(str(k) for k in doc.keys())
            raw_fields.discard("_id")  # always present, handled separately by framework

            # Normalise camelCase → snake_case (same transform the framework applies)
            seen_fields = {_camel_to_snake(f) for f in raw_fields} | raw_fields

            missing = sorted(schema_fields - seen_fields)
            extra = sorted(
                raw_fields - schema_fields - {"__v", "_cls", "_cls"}
            )  # skip Mongo internals

            if missing or extra:
                errors.append(
                    MongoCheckError(
                        pipeline_key=key,
                        input_name=input_name,
                        db=spec.db,
                        collection=spec.collection,
                        missing_fields=missing,
                        extra_fields=extra,
                    )
                )
            else:
                print(
                    f"  ✓ {key}: {spec.db}.{spec.collection} "
                    f"({len(docs)} docs sampled, {len(schema_fields)} schema fields all present)"
                )

    return errors


def verify_local(
    tables_root: Path | None = None,
    contract_fetcher: ContractFetcher | None = None,
) -> list[ContractError]:
    """Schema-only check. No Spark. Requires access to the contracts store.

    For each registered pipeline, inspects ``ContractSource`` / ``TableSource``
    annotations and asserts that the published contract exists and is
    schema-compatible. An upstream produced by another pipeline in the same
    bundle is validated against that local producer instead of the published
    contract — the upload refreshes that contract atomically.
    """
    discover_all_pipelines(tables_root)
    fetcher = contract_fetcher or _default_fetcher()
    pipelines = all_pipelines()
    local_tables: dict[str, PipelineMeta] = {
        meta.table_name: meta for meta in pipelines.values()
    }
    errors: list[ContractError] = []
    for key, meta in pipelines.items():
        errors.extend(_check_pipeline_contracts(key, meta, fetcher, local_tables))
    return errors


def _run_pipeline_and_check(
    key: str, meta: PipelineMeta, mode: str, export_dir: Path | None
) -> list[VerificationError]:
    from poorbricks.runner import run

    errors: list[VerificationError] = []

    runner_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")

    try:
        result = run(runner_key, mode=mode, skip_checks=True)
    except Exception as exc:
        return [
            VerificationError(pipeline_key=key, category="run_error", message=str(exc))
        ]

    df = result.df
    if df is None:
        return [
            VerificationError(
                pipeline_key=key,
                category="run_error",
                message="pipeline returned no DataFrame",
            )
        ]

    try:
        meta.model.verify(df)  # type: ignore[attr-defined]
    except Exception as exc:
        errors.append(
            VerificationError(pipeline_key=key, category="rule", message=str(exc))
        )

    expectations_cls = _find_expectations_for(meta)
    if expectations_cls is not None:
        for violation in expectations_cls.check(df, enforce_min_rows=False):  # type: ignore[attr-defined]
            errors.append(
                VerificationError(
                    pipeline_key=key, category="expectation", message=violation
                )
            )

    if export_dir is not None:
        export_dir.mkdir(parents=True, exist_ok=True)
        from utils.contracts import profile_dataframe

        profile = profile_dataframe(df)
        out = export_dir / f"{meta.table_name}.json"
        out.write_text(json.dumps(profile, indent=2, default=str))

    return errors


def _find_expectations_for(meta: PipelineMeta) -> type | None:
    """Locate the Expectations subclass declared in the pipeline's config.py."""
    import importlib
    import inspect

    config_module_path = meta.module.removesuffix(".pipeline") + ".config"
    try:
        module = importlib.import_module(config_module_path)
    except ImportError:
        return None
    for _, obj in inspect.getmembers(module):
        if (
            inspect.isclass(obj)
            and obj.__name__ != "Expectations"
            and any(base.__name__ == "Expectations" for base in obj.__mro__[1:])
        ):
            return obj
    return None


def _stop_spark_if_running() -> None:
    """Stop the active SparkSession to release JVM heap between batches."""
    try:
        from pyspark.sql import SparkSession

        active = SparkSession.getActiveSession()
        if active is not None:
            active.stop()
    except Exception:
        pass


def verify_ci(
    tables_root: Path | None = None,
    export_dir: Path | None = None,
    mode: str = "production",
    spark_batch_size: int = 4,
) -> list[VerificationError]:
    """Full execution. Runs each pipeline, checks rules + expectations.

    Does NOT write pipeline output to any sink. ``mode="fixtures"`` lets
    tests run without a live MongoDB; CI uses the default ``"production"``.

    Pipelines are processed in batches of ``spark_batch_size``; the Spark
    session is restarted between batches to prevent JVM heap exhaustion when
    verifying large table repositories.
    """
    discover_all_pipelines(tables_root)
    errors: list[VerificationError] = []
    pipeline_items = list(all_pipelines().items())
    for batch_start in range(0, len(pipeline_items), spark_batch_size):
        batch = pipeline_items[batch_start : batch_start + spark_batch_size]
        for key, meta in batch:
            errors.extend(_run_pipeline_and_check(key, meta, mode, export_dir))
        _stop_spark_if_running()
    return errors


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="poorbricks verify",
        description="Verify table-repo contracts and expectations",
    )
    parser.add_argument(
        "--mode", choices=["local", "ci", "arch", "mongo"], required=True
    )
    parser.add_argument(
        "--tables-root",
        type=Path,
        default=None,
        help="Override tables directory (else TABLES_ROOT or CWD/tables)",
    )
    parser.add_argument(
        "--export-dir",
        type=Path,
        default=None,
        help="(ci mode) directory to write profiler JSON files",
    )
    parser.add_argument(
        "--ci-mode",
        default="production",
        choices=["production", "fixtures"],
        help="(ci mode) runner mode used to source upstream data",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=100,
        help="(mongo mode) number of oldest + newest docs to sample per collection",
    )
    parser.add_argument(
        "--contract-url",
        default=_DEFAULT_CONTRACT_URL,
        help=(
            "(local mode) base URL of the poorbricks server to fetch contracts "
            "from. Defaults to the internal Tailscale endpoint; pass empty to "
            "use settings.contracts_api_url."
        ),
    )
    args = parser.parse_args(argv)

    if args.mode == "local":
        fetcher = _http_fetcher(args.contract_url) if args.contract_url else None
        errors: list[Any] = verify_local(
            tables_root=args.tables_root, contract_fetcher=fetcher
        )
    elif args.mode == "arch":
        errors = check_architecture(tables_root=args.tables_root)
    elif args.mode == "mongo":
        errors = verify_mongo(
            tables_root=args.tables_root,
            sample_size=args.sample_size,
        )
    else:
        errors = verify_ci(
            tables_root=args.tables_root,
            export_dir=args.export_dir,
            mode=args.ci_mode,
        )

    if not errors:
        print(f"✓ verify --mode {args.mode}: all checks passed")
        sys.exit(0)

    for err in errors:
        print(err.format())
    print(f"\n{len(errors)} failure(s)")
    sys.exit(1)


__all__ = [
    "ContractError",
    "MongoCheckError",
    "VerificationError",
    "main",
    "verify_ci",
    "verify_local",
    "verify_mongo",
    "ArchError",
    "check_architecture",
    "_DEFAULT_CONTRACT_URL",
    "_http_fetcher",
]

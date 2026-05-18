"""Contract and expectations verification for a table-repo.

Two modes:

* ``verify_local`` — schema-only check against the contracts store. Fast, no
  Spark. Detects: missing contracts; for ``TableSource`` inputs, schema
  drift between the local model and the published contract.

* ``verify_ci`` — full pipeline execution against fixtures (or production
  data), then runs ``ValidatedStruct`` rules and ``Expectations`` checks.
  Optionally exports a profile JSON per pipeline. Does not write.

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
from poorbricks.inputs import ContractSource, TableSource
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


def _default_fetcher() -> ContractFetcher:
    from utils.contracts import fetch_contract

    return fetch_contract


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
    key: str, meta: PipelineMeta, fetcher: ContractFetcher
) -> list[ContractError]:
    errors: list[ContractError] = []
    for input_name, spec in meta.inputs_cls.sources().items():
        if isinstance(spec, ContractSource):
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
            try:
                contract = fetcher(spec.table_name)
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
            diffs = _compare_schemas(spec.model.to_struct(), contract["schema_json"])
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


def verify_local(
    tables_root: Path | None = None,
    contract_fetcher: ContractFetcher | None = None,
) -> list[ContractError]:
    """Schema-only check. No Spark. Requires access to the contracts store.

    For each registered pipeline, inspects ``ContractSource`` / ``TableSource``
    annotations and asserts that the published contract exists and is
    schema-compatible.
    """
    discover_all_pipelines(tables_root)
    fetcher = contract_fetcher or _default_fetcher()
    errors: list[ContractError] = []
    for key, meta in all_pipelines().items():
        errors.extend(_check_pipeline_contracts(key, meta, fetcher))
    return errors


def _run_pipeline_and_check(
    key: str, meta: PipelineMeta, mode: str, export_dir: Path | None
) -> list[VerificationError]:
    from poorbricks.runner import run

    errors: list[VerificationError] = []

    runner_key = meta.module.removeprefix("tables.").removesuffix(".pipeline")

    try:
        result = run(runner_key, mode=mode)
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


def verify_ci(
    tables_root: Path | None = None,
    export_dir: Path | None = None,
    mode: str = "production",
) -> list[VerificationError]:
    """Full execution. Runs each pipeline, checks rules + expectations.

    Does NOT write pipeline output to any sink. ``mode="fixtures"`` lets
    tests run without a live MongoDB; CI uses the default ``"production"``.
    """
    discover_all_pipelines(tables_root)
    errors: list[VerificationError] = []
    for key, meta in all_pipelines().items():
        errors.extend(_run_pipeline_and_check(key, meta, mode, export_dir))
    return errors


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="poorbricks verify",
        description="Verify table-repo contracts and expectations",
    )
    parser.add_argument("--mode", choices=["local", "ci", "arch"], required=True)
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
    args = parser.parse_args(argv)

    if args.mode == "local":
        errors: list[Any] = verify_local(tables_root=args.tables_root)
    elif args.mode == "arch":
        errors = check_architecture(tables_root=args.tables_root)
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
    "VerificationError",
    "main",
    "verify_ci",
    "verify_local",
    "ArchError",
    "check_architecture",
]

"""Static + contract-level health checks for the poorbricks ecosystem.

Each check is a pure function over (1) the contracts persisted in
``poorbricks.data_contracts`` and (2) the source repos available on disk
(when applicable). No Spark, no DLT. The streamlit Lineage page imports
:func:`collect_findings` to render a Health panel; CI imports the
``--strict`` CLI to gate ``tools/poorbricks-upload``.

Background: a 2026-05-24 audit found the following classes of issue in
production. Each check below catches one class.

  * **Ghost contracts** — contract pushed once, source files later
    deleted. ``fact_saps``, ``fact_whodas``,
    ``fact_account_monthly_status`` all hit this state; consumers got
    empty rows in prod with no warning.
  * **Literal-NULL columns** — silvers like ``fact_patient_profile``
    declared 28 columns but populated 17 of them with
    ``f.lit(None).cast(...)``. Schema-as-aspiration, not contract.
  * **Orphan silvers** — silvers with zero downstream consumer. Often
    means the wire-up never landed (the silver is materialised at cost
    but never read).
  * **Soft PK** — declared "primary key" column is nullable / no
    ``unique_keys`` declaration. Lets duplicate rows in silently.
  * **Weak contract** — facts without freshness / null-rate / enum
    guards. Lets data drift silently.

Usage::

    python -m poorbricks.diagnostics              # human-readable report
    python -m poorbricks.diagnostics --strict     # exit 1 on any ERROR
    python -m poorbricks.diagnostics --json       # JSON output for CI
"""

from __future__ import annotations

import ast
import dataclasses
import enum
import json
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .contracts import _client
from .lineage import build_lineage_graph


def _fetch_full_contracts() -> list[dict[str, Any]]:
    """Pull every contract with all fields (no projection).

    :func:`utils.contracts.list_contract_details` projects a subset
    (table_name, level, inputs, profile.row_count) for the streamlit
    status dashboard; the diagnostics checks need more — module,
    fields, expectations, full profile. We do our own query.
    """
    from poorbricks.settings import settings

    coll = _client()[settings.contracts_db][settings.contracts_collection]
    return list(coll.find({}))


class Severity(enum.Enum):
    """Severity tiers. ERROR fails ``--strict`` mode."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclasses.dataclass(frozen=True)
class Finding:
    """A single diagnostic finding."""

    check: str
    severity: Severity
    table: str
    message: str
    details: dict[str, Any] = dataclasses.field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "severity": self.severity.value,
            "table": self.table,
            "message": self.message,
            "details": self.details,
        }


# ---------------------------------------------------------------------------
# Repo discovery
# ---------------------------------------------------------------------------


def _workspace_roots() -> list[Path]:
    """Search roots for ``tables/.../transform.py`` lookups.

    The diagnostics module lives inside ``poorbricks/``. Walk one level up
    to find sibling repos that publish contracts (silver/, watson/, …).
    """
    here = Path(__file__).resolve().parent.parent
    workspace = here.parent  # gold-pipelines/
    candidates = [workspace]
    candidates.extend(p for p in workspace.iterdir() if p.is_dir())
    return candidates


def _module_path_to_source(module: str) -> Path | None:
    """Resolve a contract's ``module`` string to its ``transform.py`` path.

    Contracts store ``module`` like ``tables.silver.fact_saps.pipeline``;
    the corresponding transform sits at ``tables/silver/fact_saps/transform.py``
    *inside one of the workspace repos*. Returns the first hit or None.
    """
    if not module:
        return None
    parts = module.split(".")
    if parts and parts[-1] in {"pipeline", "transform"}:
        parts = parts[:-1]
    rel = Path(*parts) / "transform.py"
    for root in _workspace_roots():
        candidate = root / rel
        if candidate.is_file():
            return candidate
        # Also try without the leading "tables" segment in case the
        # contract was registered from a nested repo (rare).
    return None


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def check_ghost_contracts(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Contract published but source ``transform.py`` missing on disk.

    Catches the 2026-05-24 issue where fact_saps/fact_whodas/
    fact_account_monthly_status had pushed contracts with no producer.

    Severity is tiered by level:
      * silver / gold → ERROR (these contracts must have a producer in
        this workspace; they are the ones whose absence breaks
        downstream consumers).
      * bronze → WARNING (bronze sources legitimately live in sibling
        workspaces — deadpool/, hermes/, hawkeye/, jarvis/, etc. — that
        may not be cloned next to this one. A bronze "ghost" from
        gold-pipelines/'s perspective is usually a missing workspace
        clone rather than a deleted producer).
    """
    for c in contracts:
        module = c.get("module") or ""
        if not module:
            continue
        if _module_path_to_source(module) is not None:
            continue
        level = c.get("level") or ""
        severity = (
            Severity.WARNING if level == "bronze" else Severity.ERROR
        )
        yield Finding(
            check="ghost_contract",
            severity=severity,
            table=c["table_name"],
            message=(
                f"Contract published but source not found on disk "
                f"(module={module}). "
                + (
                    "Source likely lives in a sibling workspace not "
                    "cloned next to this one."
                    if level == "bronze"
                    else "Restore the producer or delete the orphaned contract."
                )
            ),
            details={"module": module, "level": level},
        )


def check_orphan_silvers(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Silver tables with zero downstream consumers."""
    _, edges = build_lineage_graph(contracts)
    consumers: dict[str, set[str]] = defaultdict(set)
    for e in edges:
        consumers[e.source].add(e.target)
    for c in contracts:
        if c.get("level") != "silver":
            continue
        name = c["table_name"]
        if not consumers.get(name):
            yield Finding(
                check="orphan_silver",
                severity=Severity.WARNING,
                table=name,
                message=(
                    "Silver table has no downstream consumer in the published "
                    "contract set. Either wire a gold consumer or archive it."
                ),
            )


def check_literal_null_columns(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Transforms with a high ratio of ``f.lit(None)`` projected columns.

    Walks the AST of ``transform.py``; counts the number of
    ``f.lit(None).cast(...)`` expressions appearing as ``.alias(...)``
    children of a top-level ``select(...)``. If more than 30% of declared
    output columns are literal-null, flag.
    """
    for c in contracts:
        path = _module_path_to_source(c.get("module") or "")
        if path is None:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        literal_null_count = _count_literal_null_aliases(tree)
        total_cols = len(c.get("fields") or [])
        if total_cols < 5 or literal_null_count == 0:
            continue
        ratio = literal_null_count / total_cols
        if ratio >= 0.30:
            yield Finding(
                check="literal_null_columns",
                severity=Severity.WARNING,
                table=c["table_name"],
                message=(
                    f"{literal_null_count}/{total_cols} output columns "
                    f"({ratio:.0%}) are hardcoded f.lit(None) — schema is "
                    f"aspirational, not produced."
                ),
                details={
                    "literal_null_count": literal_null_count,
                    "total_cols": total_cols,
                    "path": str(path),
                },
            )


def _count_literal_null_aliases(tree: ast.Module) -> int:
    """Count ``f.lit(None).cast(...).alias(...)`` patterns anywhere in the AST.

    A ``Call`` whose function is an ``Attribute`` named ``alias`` and whose
    immediate receiver chain contains ``f.lit(None)``.
    """
    count = 0
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "alias"
            and _expression_starts_with_lit_none(node.func.value)
        ):
            count += 1
    return count


def _expression_starts_with_lit_none(expr: ast.AST) -> bool:
    """True if ``expr`` (or any of its inner receivers) is ``f.lit(None)``."""
    cur: ast.AST | None = expr
    while isinstance(cur, ast.Call):
        if (
            isinstance(cur.func, ast.Attribute)
            and cur.func.attr == "lit"
            and len(cur.args) == 1
            and isinstance(cur.args[0], ast.Constant)
            and cur.args[0].value is None
        ):
            return True
        if isinstance(cur.func, ast.Attribute):
            cur = cur.func.value
            continue
        break
    return False


def check_soft_primary_keys(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Declared PK is nullable / no unique_keys."""
    for c in contracts:
        if c.get("level") == "bronze":
            continue
        expectations = c.get("expectations") or {}
        unique_keys = expectations.get("unique_keys") or []
        non_null = set(expectations.get("non_null_columns") or [])
        if not unique_keys:
            yield Finding(
                check="soft_pk_no_unique_keys",
                severity=Severity.WARNING,
                table=c["table_name"],
                message="Expectations.UNIQUE_KEYS is empty — duplicate rows allowed silently.",
            )
            continue
        # First unique-key group is the canonical PK.
        pk_cols = unique_keys[0]
        for col_name in pk_cols:
            field = next(
                (f for f in c.get("fields") or [] if f.get("name") == col_name),
                None,
            )
            if field and field.get("nullable") and col_name not in non_null:
                yield Finding(
                    check="soft_pk_nullable",
                    severity=Severity.WARNING,
                    table=c["table_name"],
                    message=(
                        f"PK column {col_name!r} is nullable and not in "
                        f"NON_NULL_COLUMNS — partial PK semantics."
                    ),
                    details={"pk": pk_cols},
                )


def check_freshness_declared(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Silver/gold facts without FRESH_COLUMN."""
    for c in contracts:
        if c.get("level") == "bronze":
            continue
        name = c["table_name"]
        # Dims commonly lack a fresh column today; downgrade severity.
        is_dim = name.startswith("dim_")
        expectations = c.get("expectations") or {}
        if not expectations.get("fresh_column"):
            yield Finding(
                check="missing_freshness",
                severity=Severity.INFO if is_dim else Severity.WARNING,
                table=name,
                message=(
                    "No FRESH_COLUMN declared — stale data won't trip an alarm."
                ),
            )


def check_empty_row_count(contracts: list[dict[str, Any]]) -> Iterable[Finding]:
    """Contract carries ``profile.row_count == 0``."""
    for c in contracts:
        profile = c.get("profile") or {}
        if profile.get("row_count") == 0:
            yield Finding(
                check="empty_row_count",
                severity=Severity.WARNING,
                table=c["table_name"],
                message=(
                    "Last published profile reported row_count=0 — either "
                    "the transform is a stub or the inputs are dry."
                ),
            )


_CHECKS = (
    check_ghost_contracts,
    check_orphan_silvers,
    check_literal_null_columns,
    check_soft_primary_keys,
    check_freshness_declared,
    check_empty_row_count,
)


def collect_findings(
    contracts: list[dict[str, Any]] | None = None,
) -> list[Finding]:
    """Run all checks and return findings sorted (severity, check, table)."""
    if contracts is None:
        contracts = _fetch_full_contracts()
    findings: list[Finding] = []
    for check in _CHECKS:
        findings.extend(check(contracts))
    severity_order = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
    findings.sort(key=lambda f: (severity_order[f.severity], f.check, f.table))
    return findings


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _print_human(findings: list[Finding]) -> None:
    if not findings:
        print("✓ No findings.")
        return
    by_sev: dict[Severity, list[Finding]] = defaultdict(list)
    for f in findings:
        by_sev[f.severity].append(f)
    for sev in (Severity.ERROR, Severity.WARNING, Severity.INFO):
        bucket = by_sev.get(sev, [])
        if not bucket:
            continue
        print(f"\n=== {sev.value.upper()} ({len(bucket)}) ===")
        for f in bucket:
            print(f"  [{f.check}] {f.table}: {f.message}")
            for k, v in f.details.items():
                print(f"      {k} = {v}")


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="poorbricks.diagnostics")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero if any ERROR-severity finding is reported.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable text.",
    )
    args = parser.parse_args(argv)

    findings = collect_findings()

    if args.json:
        print(json.dumps([f.as_dict() for f in findings], indent=2))
    else:
        _print_human(findings)

    if args.strict and any(f.severity is Severity.ERROR for f in findings):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

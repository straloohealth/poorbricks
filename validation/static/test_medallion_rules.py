"""Medallion architecture rules — AST-based pytest tests.

Enforces the bronze/silver/gold contracts on every pipeline under
``source/pipelines/{bronze,silver,gold}/``. Each rule has a temporary
file-path allowlist that captures the current legacy violators so the
suite passes today; new violations land as test failures in CI.

Cleanup of every allowlist entry is tracked in the medallion cleanup
plan ("PR 2 onwards"). When a pipeline is migrated, drop its line from
the relevant ALLOWLIST entry — the test will then enforce the rule for
that file going forward.

Rules summary:
    1. Bronze inputs must be MongoSource (raw Mongo). Fivetran-managed
       TableSource mirrors are temporarily allowlisted.
    2. Silver inputs must be Delta TableSource pointing at .bronze. /
       .silver. Legacy PostgresTableSource readers are allowlisted.
    3. Gold inputs must be Delta TableSource pointing at .silver. /
       .gold. Legacy MongoSource / PostgresTableSource / .master.
       readers are allowlisted.
    4. No pipeline file imports psycopg2 / sqlalchemy / PostgresLoader.
    5. Bronze pipelines must not declare storage="postgres". Silver may
       (it is mirrored to Postgres alongside gold; bronze stays Delta-only).
    6. Pipelines under gold/ (and any pipeline declaring level="gold")
       must declare storage="postgres".
    7. artifacts/lineage.json: every node has level in
       {"bronze","silver","gold"}.
    8. (Pending) account_monthly_report is unified — placeholder skip.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Repository roots
# ---------------------------------------------------------------------------

REPO_ROOT: Path = Path(__file__).resolve().parent.parent.parent.parent  # poorbricks/
PIPELINES_DIR: Path = REPO_ROOT / "source" / "pipelines"
LINEAGE_PATH: Path = REPO_ROOT / "artifacts" / "lineage.json"

ALLOWED_LEVELS: frozenset[str] = frozenset({"bronze", "silver", "gold"})

# ---------------------------------------------------------------------------
# Allowlists — populated empirically from current main; each entry's cleanup
# is tracked in the medallion cleanup plan. Drop entries as pipelines migrate.
# ---------------------------------------------------------------------------

# TODO: cleanup tracked in medallion-cleanup plan "Bronze sources" section.
# Bronze pipelines temporarily reading Fivetran-managed Delta mirrors
# (poorbricks_dev.master.*) instead of MongoSource. Will switch to MongoSource
# in PR 3 once the per-source secrets are wired through.
_TEST1_BRONZE_NON_MONGO_ALLOWLIST: set[str] = {
    "source/pipelines/bronze/chandler/camed/pipeline.py",
    "source/pipelines/bronze/friday/appointments/pipeline.py",
    "source/pipelines/bronze/hermes/emails/pipeline.py",
    "source/pipelines/bronze/hermes/messages/pipeline.py",
    "source/pipelines/bronze/hermes/tickets/pipeline.py",
    "source/pipelines/bronze/jarvis/calldetails/pipeline.py",
    "source/pipelines/bronze/smith/navigators/pipeline.py",
    "source/pipelines/bronze/smith/users/pipeline.py",
    "source/pipelines/bronze/watson/events/pipeline.py",
    "source/pipelines/bronze/watson/notes/pipeline.py",
    "source/pipelines/bronze/watson/records/pipeline.py",
}

# Silver pipelines reading bronze/silver via PostgresTableSource. Fully
# drained — every silver pipeline now reads ``poorbricks_dev.bronze.*`` /
# ``poorbricks_dev.silver.*`` via Delta ``TableSource``. Kept as an empty
# allowlist so any regression fails the test immediately.
_TEST2_SILVER_NON_DELTA_ALLOWLIST: set[str] = set()

# TODO: cleanup tracked in medallion-cleanup plan "Gold sources" section.
# Gold pipelines that still read from poorbricks_dev.master.*, mongo collections,
# or Postgres bronze/silver instead of the silver/gold Delta layer. PR 5 will
# point each one at its proper silver upstream.
_TEST3_GOLD_NON_DELTA_ALLOWLIST: set[str] = {
    "source/pipelines/gold/calls_and_evaluation_metrics/pipeline.py",
    "source/pipelines/gold/descriptions/pipeline.py",
    "source/pipelines/gold/dictionary/pipeline.py",
}

# TODO: cleanup tracked in medallion-cleanup plan "Postgres-client imports".
# No current violators — keep as an empty allowlist so any new direct import
# fails the test immediately.
_TEST4_POSTGRES_CLIENT_ALLOWLIST: set[str] = set()

# Bronze pipelines that still declare ``storage="postgres"``. Silver
# pipelines are no longer checked (silver is now mirrored to Postgres
# alongside gold — the export job iterates every silver pipeline by
# level, regardless of whether it declares the storage kwarg).
_TEST5_NON_DELTA_STORAGE_ALLOWLIST: set[str] = {
    # TODO: drain after PR-hermes-retirement
    "source/pipelines/bronze/hermes/messages/pipeline.py",
    # TODO: drain after the legacy communication.messages.deadpool
    # pipeline retires. Both pipelines write to the same logical
    # `deadpool_messages` Delta table; setting storage="postgres" here
    # disambiguates the framework registry until the legacy one is gone.
    "source/pipelines/bronze/deadpool/messages/pipeline.py",
}

# Empty: every pipeline outside ``source/pipelines/legacy/`` that declares
# ``level="gold"`` (or lives under ``source/pipelines/gold/``) now declares
# ``storage="postgres"``. Legacy mirrors are skipped by the test walker
# itself (see ``test_gold_storage_is_postgres``), not allowlisted here.
_TEST6_GOLD_NOT_POSTGRES_ALLOWLIST: set[str] = set()

# TODO: cleanup tracked in medallion-cleanup plan "Lineage levels".
# No current violators — every node already has a valid level. The allowlist
# is keyed by lineage table_name (not file path) so future failures can be
# pinpointed.
_TEST7_LINEAGE_LEVEL_ALLOWLIST: set[str] = set()

# TODO: drain in Round 6 (retire passthroughs) and via dedicated PRs for the
# remaining real aggregators. 13 entries:
#   * 9 per-account passthroughs in gold/ that re-export a single
#     poorbricks_dev.master.<account>_monthly_report table verbatim — these get
#     deleted when Round 6 moves the legacy upstream pipelines under legacy/.
#   * 4 real aggregators (account_monthly_kpi, calls_and_evaluation_metrics,
#     descriptions, roi_report) that need their own silver builds before they
#     can stop reading poorbricks_dev.master.*.
# The companion `gold_reads_master` / `gold_reads_framework_master` metrics in
# test_medallion_metrics.py ratchet the same surface. ``gold/dictionary`` was
# drained by routing its registry walk through ``meta.catalog.compute``
# instead of reading the persisted ``framework_master.poorbricks_catalog``
# Delta table.
_TEST_MASTER_ALLOWLIST: set[str] = {
    "source/pipelines/gold/aco_cearense_monthly_report/pipeline.py",
    "source/pipelines/gold/aon_monthly_report/pipeline.py",
    "source/pipelines/gold/aon_monthly_summary/pipeline.py",
    "source/pipelines/gold/cafaz_monthly_report/pipeline.py",
    "source/pipelines/gold/camed_monthly_report/pipeline.py",
    "source/pipelines/gold/descriptions/pipeline.py",
    "source/pipelines/gold/ge_monthly_report/pipeline.py",
    "source/pipelines/gold/rede_sc_monthly_report/pipeline.py",
    "source/pipelines/gold/sepaco_monthly_report/pipeline.py",
    "source/pipelines/gold/unimed_monthly_report/pipeline.py",
}


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------


def _iter_pipeline_py_files(layer: str) -> list[Path]:
    """Return sorted pipeline.py paths under ``source/pipelines/<layer>``."""
    layer_dir = PIPELINES_DIR / layer
    if not layer_dir.exists():
        return []
    return sorted(
        p for p in layer_dir.rglob("pipeline.py") if "__pycache__" not in p.parts
    )


def _iter_all_pipeline_py_files() -> list[Path]:
    """Return every pipeline.py under ``source/pipelines/``."""
    if not PIPELINES_DIR.exists():
        return []
    return sorted(
        p for p in PIPELINES_DIR.rglob("pipeline.py") if "__pycache__" not in p.parts
    )


def _iter_all_py_files() -> list[Path]:
    """Return every .py file under ``source/pipelines/``."""
    if not PIPELINES_DIR.exists():
        return []
    return sorted(
        p for p in PIPELINES_DIR.rglob("*.py") if "__pycache__" not in p.parts
    )


def _parse(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"))


def _rel(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def _source_class_name(call: ast.Call) -> str | None:
    """Return ``"MongoSource"``, ``"TableSource"``, ``"PostgresTableSource"``
    if ``call`` is a call to one of those, else None."""
    func = call.func
    name: str | None = None
    if isinstance(func, ast.Name):
        name = func.id
    elif isinstance(func, ast.Attribute):
        name = func.attr
    if name in {"MongoSource", "TableSource", "PostgresTableSource"}:
        return name
    return None


def _iter_inputs_class_sources(
    tree: ast.AST,
) -> list[tuple[str, str, ast.Call]]:
    """Yield ``(field_name, source_class_name, call_node)`` for every
    ``Annotated[DataFrame, <Source>(...)]`` declared inside a class that
    subclasses ``Inputs``.
    """
    out: list[tuple[str, str, ast.Call]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        is_inputs = any(
            (isinstance(b, ast.Name) and b.id == "Inputs")
            or (isinstance(b, ast.Attribute) and b.attr == "Inputs")
            for b in node.bases
        )
        if not is_inputs:
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            ann = stmt.annotation
            if not isinstance(ann, ast.Subscript):
                continue
            ann_value = ann.value
            is_annotated = (
                isinstance(ann_value, ast.Name) and ann_value.id == "Annotated"
            ) or (
                isinstance(ann_value, ast.Attribute) and ann_value.attr == "Annotated"
            )
            if not is_annotated:
                continue
            slice_node = ann.slice
            elements: list[ast.expr]
            if isinstance(slice_node, ast.Tuple):
                elements = list(slice_node.elts)
            else:
                elements = [slice_node]
            for elt in elements:
                if not isinstance(elt, ast.Call):
                    continue
                src_name = _source_class_name(elt)
                if src_name is None:
                    continue
                field_name = (
                    stmt.target.id if isinstance(stmt.target, ast.Name) else "?"
                )
                out.append((field_name, src_name, elt))
    return out


def _table_source_qualified_name(call: ast.Call) -> str | None:
    """Return the first positional / ``table_name=`` kwarg of a TableSource
    call as a literal string, or None if it isn't a string literal."""
    if call.args:
        first = call.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            return first.value
    for kw in call.keywords:
        if kw.arg == "table_name" and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, str):
                return value
    return None


def _table_source_layer(call: ast.Call) -> str | None:
    """Return the medallion segment (second dotted token) of a TableSource
    qualified name, or None when the name is dynamic / not a literal.

    A literal name like ``"poorbricks_dev.bronze.smith_navigators"`` returns
    ``"bronze"``.
    """
    qualified = _table_source_qualified_name(call)
    if qualified is None:
        return None
    parts = qualified.split(".")
    if len(parts) < 2:
        return None
    return parts[1]


def _postgres_source_schema(call: ast.Call) -> str | None:
    """Return the ``schema=`` kwarg of a PostgresTableSource call as a
    literal string, or None."""
    for kw in call.keywords:
        if kw.arg == "schema" and isinstance(kw.value, ast.Constant):
            value = kw.value.value
            if isinstance(value, str):
                return value
    return None


def _pipeline_decorator_kwargs(tree: ast.AST) -> dict[str, object]:
    """Return literal kwargs of the first ``@pipeline(...)`` decorator
    found in ``tree``. Non-literal values become the sentinel string
    ``"<expr>"``."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            func = dec.func
            is_pipeline = (isinstance(func, ast.Name) and func.id == "pipeline") or (
                isinstance(func, ast.Attribute) and func.attr == "pipeline"
            )
            if not is_pipeline:
                continue
            kwargs: dict[str, object] = {}
            for kw in dec.keywords:
                if kw.arg is None:
                    continue
                if isinstance(kw.value, ast.Constant):
                    kwargs[kw.arg] = kw.value.value
                else:
                    kwargs[kw.arg] = "<expr>"
            return kwargs
    return {}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_bronze_inputs_are_mongo_only() -> None:
    """Every bronze pipeline.py must declare its inputs as ``MongoSource``.

    Fivetran-managed Delta-mirror exceptions live in
    ``_TEST1_BRONZE_NON_MONGO_ALLOWLIST`` and are scheduled for removal in
    the medallion cleanup plan.
    """
    violations: list[str] = []
    for path in _iter_pipeline_py_files("bronze"):
        rel = _rel(path)
        if rel in _TEST1_BRONZE_NON_MONGO_ALLOWLIST:
            continue
        tree = _parse(path)
        for field, src, _call in _iter_inputs_class_sources(tree):
            if src != "MongoSource":
                violations.append(
                    f"{rel}: input '{field}' uses {src} (bronze must use MongoSource)"
                )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Bronze pipelines must declare inputs as MongoSource(...).",
                    "Either change the input or add the file to "
                    "_TEST1_BRONZE_NON_MONGO_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_silver_inputs_are_bronze_delta_only() -> None:
    """Silver inputs must be ``TableSource(...)`` with a qualified name in
    ``.bronze.`` or ``.silver.`` (within-layer joins are allowed)."""
    violations: list[str] = []
    for path in _iter_pipeline_py_files("silver"):
        rel = _rel(path)
        if rel in _TEST2_SILVER_NON_DELTA_ALLOWLIST:
            continue
        tree = _parse(path)
        for field, src, call in _iter_inputs_class_sources(tree):
            if src == "MongoSource":
                violations.append(
                    f"{rel}: input '{field}' uses MongoSource "
                    f"(silver must use Delta TableSource on .bronze. / .silver.)"
                )
                continue
            if src == "PostgresTableSource":
                violations.append(
                    f"{rel}: input '{field}' uses PostgresTableSource "
                    f"(silver must read Delta TableSource on .bronze. / .silver.)"
                )
                continue
            if src == "TableSource":
                layer = _table_source_layer(call)
                if layer is None:
                    violations.append(
                        f"{rel}: input '{field}' uses TableSource with a "
                        f"non-literal qualified name (cannot verify layer)"
                    )
                elif layer not in {"bronze", "silver"}:
                    violations.append(
                        f"{rel}: input '{field}' reads .{layer}. "
                        f"(silver may only read .bronze. / .silver.)"
                    )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Silver pipelines must read .bronze. / .silver. Delta tables.",
                    "Either fix the input or add the file to "
                    "_TEST2_SILVER_NON_DELTA_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_gold_inputs_are_silver_or_gold_delta_only() -> None:
    """Gold inputs must be ``TableSource(...)`` with a qualified name in
    ``.silver.`` or ``.gold.``. No ``MongoSource``, no
    ``PostgresTableSource``, no ``.bronze.``."""
    violations: list[str] = []
    for path in _iter_pipeline_py_files("gold"):
        rel = _rel(path)
        if rel in _TEST3_GOLD_NON_DELTA_ALLOWLIST:
            continue
        tree = _parse(path)
        for field, src, call in _iter_inputs_class_sources(tree):
            if src == "MongoSource":
                violations.append(
                    f"{rel}: input '{field}' uses MongoSource "
                    f"(gold must read Delta TableSource on .silver. / .gold.)"
                )
                continue
            if src == "PostgresTableSource":
                violations.append(
                    f"{rel}: input '{field}' uses PostgresTableSource "
                    f"(gold must read Delta TableSource on .silver. / .gold.)"
                )
                continue
            if src == "TableSource":
                layer = _table_source_layer(call)
                if layer is None:
                    violations.append(
                        f"{rel}: input '{field}' uses TableSource with a "
                        f"non-literal qualified name (cannot verify layer)"
                    )
                elif layer not in {"silver", "gold"}:
                    violations.append(
                        f"{rel}: input '{field}' reads .{layer}. "
                        f"(gold may only read .silver. / .gold.)"
                    )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Gold pipelines must read .silver. / .gold. Delta tables.",
                    "Either fix the input or add the file to "
                    "_TEST3_GOLD_NON_DELTA_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_no_pipeline_imports_postgres_client() -> None:
    """No ``.py`` under ``source/pipelines/`` may import ``psycopg2``,
    ``sqlalchemy``, or the project's ``PostgresLoader``. JDBC / Postgres
    materialization is the framework's job."""
    forbidden_modules: frozenset[str] = frozenset({"psycopg2", "sqlalchemy"})
    forbidden_names: frozenset[str] = frozenset({"PostgresLoader"})
    violations: list[str] = []
    for path in _iter_all_py_files():
        rel = _rel(path)
        if rel in _TEST4_POSTGRES_CLIENT_ALLOWLIST:
            continue
        tree = _parse(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = alias.name.split(".")[0]
                    if root in forbidden_modules:
                        violations.append(f"{rel}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                module_root = (node.module or "").split(".")[0]
                if module_root in forbidden_modules:
                    violations.append(f"{rel}: from {node.module} import ...")
                for alias in node.names:
                    if alias.name in forbidden_names:
                        violations.append(
                            f"{rel}: from {node.module} import {alias.name}"
                        )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Pipeline files must not import postgres clients directly.",
                    "Use the framework (PostgresTableSource / storage='postgres') instead.",
                    "",
                    *violations,
                ]
            )
        )


def test_bronze_storage_is_delta() -> None:
    """Bronze ``@pipeline(...)`` decorators must not declare
    ``storage="postgres"`` — bronze stays Delta-only. Silver is no longer
    checked: silver pipelines are mirrored to Postgres alongside gold by
    the export job, so declaring ``storage="postgres"`` (or omitting it)
    is fine. The remaining allowlist captures two bronze edge-cases tied
    to the legacy hermes/deadpool migration."""
    violations: list[str] = []
    for path in _iter_pipeline_py_files("bronze"):
        rel = _rel(path)
        if rel in _TEST5_NON_DELTA_STORAGE_ALLOWLIST:
            continue
        kwargs = _pipeline_decorator_kwargs(_parse(path))
        if kwargs.get("storage") == "postgres":
            violations.append(
                f'{rel}: declares storage="postgres" '
                f"(bronze pipelines must materialize to Delta)"
            )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    'Bronze pipelines must not declare storage="postgres".',
                    "Either drop the kwarg or add the file to "
                    "_TEST5_NON_DELTA_STORAGE_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_gold_storage_is_postgres() -> None:
    """Gold pipelines must declare ``storage="postgres"``. This covers two
    populations: anything under ``source/pipelines/gold/`` (canonical home)
    plus anything elsewhere that declares ``level="gold"``. Pipelines under
    ``source/pipelines/legacy/`` are skipped — they're frozen mirrors awaiting
    migration, governed by the legacy=True kwarg, not by the medallion rules."""
    violations: list[str] = []
    for path in _iter_all_pipeline_py_files():
        rel = _rel(path)
        # Legacy pipelines are exempt: they live in a frozen tree that will
        # be drained pipeline-by-pipeline. Each migration moves a pipeline
        # out from under legacy/ and lands it (with storage="postgres") in
        # source/pipelines/gold/, at which point this test starts enforcing.
        if rel.startswith("source/pipelines/legacy/"):
            continue
        kwargs = _pipeline_decorator_kwargs(_parse(path))
        is_in_gold = rel.startswith("source/pipelines/gold/")
        is_gold_level = kwargs.get("level") == "gold"
        if not (is_in_gold or is_gold_level):
            continue
        if kwargs.get("storage") == "postgres":
            continue
        if rel in _TEST6_GOLD_NOT_POSTGRES_ALLOWLIST:
            continue
        reason = (
            "lives under source/pipelines/gold/"
            if is_in_gold
            else 'declares level="gold"'
        )
        violations.append(f'{rel}: {reason} but does not declare storage="postgres"')
    if violations:
        pytest.fail(
            "\n".join(
                [
                    'Gold pipelines must declare storage="postgres".',
                    "Either set it or add the file to "
                    "_TEST6_GOLD_NOT_POSTGRES_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_lineage_nodes_have_level() -> None:
    """Every entry in ``artifacts/lineage.json`` must declare a ``level``
    in ``{"bronze","silver","gold"}``. The ``lineage`` schema calls them
    "pipelines"; this test treats each as a node."""
    if not LINEAGE_PATH.exists():
        pytest.fail(
            f"{LINEAGE_PATH.relative_to(REPO_ROOT)}: missing. "
            "Run `poetry run python scripts/export_lineage.py`."
        )
    data = json.loads(LINEAGE_PATH.read_text(encoding="utf-8"))
    nodes = data.get("pipelines", [])
    violations: list[str] = []
    for node in nodes:
        name = node.get("table_name", "<unknown>")
        if name in _TEST7_LINEAGE_LEVEL_ALLOWLIST:
            continue
        level = node.get("level")
        if level not in ALLOWED_LEVELS:
            violations.append(
                f"{name}: level={level!r} (must be one of {sorted(ALLOWED_LEVELS)})"
            )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Lineage nodes are missing or have invalid level:",
                    "",
                    *violations,
                ]
            )
        )


def test_no_master_or_framework_master_reads() -> None:
    """Forbid ``TableSource`` references to ``poorbricks_dev.master.*`` or
    ``poorbricks_dev.framework_master.*`` in any non-legacy ``pipeline.py``.

    Pipelines must read bronze, silver, or gold — never the legacy Fivetran
    ``master`` schema or the ``framework_master`` schema. Existing violators
    are temporarily allowlisted in ``_TEST_MASTER_ALLOWLIST``; this allowlist
    drains as Round 6 retires legacy passthroughs and dedicated PRs land
    silver upstreams for the remaining real aggregators. The companion
    ``gold_reads_master`` / ``gold_reads_framework_master`` metrics in
    ``test_medallion_metrics.py`` ratchet the same surface.

    The walker skips ``source/pipelines/legacy/`` — those are frozen mirrors
    that are allowed to read whatever they read until the migration retires
    them.
    """
    forbidden_schemas: frozenset[str] = frozenset({"master", "framework_master"})
    violations: list[str] = []
    for path in _iter_all_pipeline_py_files():
        rel = _rel(path)
        if rel.startswith("source/pipelines/legacy/"):
            continue
        if rel in _TEST_MASTER_ALLOWLIST:
            continue
        tree = _parse(path)
        for field, src, call in _iter_inputs_class_sources(tree):
            if src != "TableSource":
                continue
            qualified = _table_source_qualified_name(call)
            layer = _table_source_layer(call)
            if layer is None or layer not in forbidden_schemas:
                continue
            violations.append(
                f"{rel}: input '{field}' reads {qualified!r} "
                f"(non-legacy pipelines must not read .{layer}.)"
            )
    if violations:
        pytest.fail(
            "\n".join(
                [
                    "Non-legacy pipelines must not read poorbricks_dev.master.* "
                    "or poorbricks_dev.framework_master.* via TableSource.",
                    "Either route through bronze/silver/gold or add the file to "
                    "_TEST_MASTER_ALLOWLIST with a TODO link.",
                    "",
                    *violations,
                ]
            )
        )


def test_account_monthly_report_is_unified() -> None:
    """Future state: the per-account ``*_monthly_report`` pipelines collapse
    into a single unified ``account_monthly_report``. Skipped until Part 2
    of the medallion cleanup plan ships."""
    pytest.skip("Pending Part 2: account_monthly_report ships")

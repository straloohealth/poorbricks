"""Runtime column-level lineage capture from the Spark analyzed plan.

After a pipeline computes its output DataFrame, ``capture_lineage`` walks the
Catalyst *analyzed* logical plan (``df._jdf.queryExecution().analyzed()``) to
work out, for each output column, which upstream source columns it derives
from. Reading the analyzed plan is metadata-only — it triggers no Spark job and
does not recompute the DataFrame.

Two tiers, by necessity:

* **Tier A (exact):** an output column that is a pass-through / rename / simple
  expression over leaf attributes is traced back to the exact ``{table,
  column}`` it reads, by following Catalyst ``exprId`` references down to leaf
  relations. This covers the bulk of medallion selects/renames.
* **Tier B (best-effort):** when an output column can't be fully resolved (an
  aggregate, a literal, a UDF, ``monotonically_increasing_id``…), the captured
  ``expression`` string and whatever leaf attributes it touches are recorded
  with ``exact=false``.

Leaf relations in the plan are anonymous, so each leaf is attributed to a
declared input by matching its column-name set against the input's schema. Only
``ContractSource`` / ``TableSource`` upstreams (which resolve to poorbricks
contracts) appear in the ``consumed`` map that cross-table contract testing
reads; ``MongoSource`` is raw external data, not a contract dependency.

Capture never raises: any plan-walk failure yields a degraded document with a
``warnings`` entry so the pipeline run is unaffected.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .inputs import ContractSource, MongoSource, TableSource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .inputs import Inputs, SourceSpec

ENGINE = "spark-analyzed-plan"

# Process-local registry of lineage captured during the current process. The
# pytest plugin reads this at session finish to check the consumed columns of
# every pipeline the tests actually exercised against published upstream
# contracts — without re-running anything.
_captured: dict[str, dict[str, Any]] = {}


def record_capture(table_name: str, lineage: dict[str, Any] | None) -> None:
    """Stash a pipeline's captured lineage for the pytest contract-check hook."""
    if lineage is not None:
        _captured[table_name] = lineage


def captured() -> dict[str, dict[str, Any]]:
    """Return lineage captured so far this process (``{table_name: lineage}``)."""
    return dict(_captured)


def clear_captured() -> None:
    _captured.clear()


def _seq(java_seq: Any) -> list[Any]:
    """Materialize a Scala ``Seq`` (exposed via py4j) into a Python list."""
    return [java_seq.apply(i) for i in range(java_seq.size())]


def _source_identity(spec: SourceSpec) -> tuple[str | None, bool]:
    """Return ``(table_identifier, is_contract_dependency)`` for a source spec.

    ``is_contract_dependency`` is True only for upstreams that resolve to a
    poorbricks contract (``ContractSource`` / ``TableSource``) — those are the
    edges cross-table contract testing verifies.
    """
    if isinstance(spec, ContractSource | TableSource):
        return spec.table_name, True
    if isinstance(spec, MongoSource):
        return f"mongo:{spec.db}.{spec.collection}", False
    # PostgresTableSource is the only remaining SourceSpec member.
    return f"{spec.schema}.{spec.table}", False


def _source_columns(spec: SourceSpec) -> set[str] | None:
    """Best-effort column-name set for a declared source (None when unknown)."""
    try:
        if isinstance(spec, TableSource):
            return {f.name for f in spec.model.to_struct().fields}
        if isinstance(spec, MongoSource):
            return {f.name for f in spec.schema.fields}
        if isinstance(spec, ContractSource):
            from pyspark.sql.types import StructType

            from utils.contracts import fetch_contract

            contract = fetch_contract(spec.table_name)
            return {f.name for f in StructType.fromJson(contract["schema_json"]).fields}
    except Exception:
        return None
    return None


def _build_source_index(
    inputs_cls: type[Inputs],
) -> list[tuple[str, str | None, bool, set[str] | None]]:
    """Return ``[(input_name, table_id, is_contract_dep, columns)]`` per source."""
    index: list[tuple[str, str | None, bool, set[str] | None]] = []
    for input_name, spec in inputs_cls.sources().items():
        table_id, is_dep = _source_identity(spec)
        index.append((input_name, table_id, is_dep, _source_columns(spec)))
    return index


def _match_leaf_to_source(
    leaf_columns: set[str],
    source_index: list[tuple[str, str | None, bool, set[str] | None]],
) -> tuple[str | None, str | None, bool]:
    """Match a leaf relation's columns to the best declared source.

    Returns ``(input_name, table_id, is_contract_dep)``; ``(None, None, False)``
    when no source has overlapping columns (or none declare a schema).
    """
    best: tuple[int, str | None, str | None, bool] = (0, None, None, False)
    for input_name, table_id, is_dep, columns in source_index:
        if not columns:
            continue
        overlap = len(leaf_columns & columns)
        if overlap > best[0]:
            best = (overlap, input_name, table_id, is_dep)
    return best[1], best[2], best[3]


def _collect_plan(
    plan: Any,
) -> tuple[dict[int, tuple[int, str]], dict[int, set[int]], list[set[str]]]:
    """Walk the analyzed plan once.

    Returns:
        leaf_attr: exprId -> (leaf_index, column_name) for every leaf attribute.
        produced_refs: exprId -> set(referenced exprIds) for every Alias.
        leaf_columns: list of column-name sets, one per leaf (index-aligned).
    """
    leaf_attr: dict[int, tuple[int, str]] = {}
    produced_refs: dict[int, set[int]] = {}
    leaf_columns: list[set[str]] = []

    def visit(node: Any) -> None:
        children = _seq(node.children())
        if not children:
            leaf_index = len(leaf_columns)
            cols: set[str] = set()
            for attr in _seq(node.output()):
                leaf_attr[attr.exprId().id()] = (leaf_index, attr.name())
                cols.add(attr.name())
            leaf_columns.append(cols)
        # Record Alias expressions (output exprId <- referenced exprIds).
        for expr in _seq(node.expressions()):
            if expr.getClass().getSimpleName() == "Alias":
                try:
                    out_id = expr.exprId().id()
                    refs = {a.exprId().id() for a in _seq(expr.references().toSeq())}
                    produced_refs[out_id] = refs
                except Exception:
                    continue
        for child in children:
            visit(child)

    visit(plan)
    return leaf_attr, produced_refs, leaf_columns


def _resolve(
    expr_id: int,
    leaf_attr: dict[int, tuple[int, str]],
    produced_refs: dict[int, set[int]],
    seen: set[int],
) -> set[tuple[int, str]]:
    """Transitively resolve an exprId to the leaf ``(leaf_index, column)`` set."""
    if expr_id in leaf_attr:
        return {leaf_attr[expr_id]}
    if expr_id in seen:
        return set()
    seen.add(expr_id)
    resolved: set[tuple[int, str]] = set()
    for ref in produced_refs.get(expr_id, set()):
        resolved |= _resolve(ref, leaf_attr, produced_refs, seen)
    return resolved


def capture_lineage(df: DataFrame, inputs_cls: type[Inputs]) -> dict[str, Any]:
    """Capture column-level lineage for ``df`` against its declared inputs.

    Returns a BSON-safe lineage document (never raises). On any failure the
    document carries an empty mapping plus a ``warnings`` entry.
    """
    doc: dict[str, Any] = {
        "engine": ENGINE,
        "columns": {},
        "consumed": {},
        "warnings": [],
    }
    try:
        analyzed = df._jdf.queryExecution().analyzed()
        source_index = _build_source_index(inputs_cls)
        leaf_attr, produced_refs, leaf_columns = _collect_plan(analyzed)

        # Attribute each leaf to a declared source by column-set overlap.
        leaf_to_source: dict[int, tuple[str | None, str | None, bool]] = {
            idx: _match_leaf_to_source(cols, source_index)
            for idx, cols in enumerate(leaf_columns)
        }

        columns: dict[str, Any] = {}
        consumed: dict[str, set[str]] = {}
        for out_attr in _seq(analyzed.output()):
            out_name = out_attr.name()
            leaves = _resolve(out_attr.exprId().id(), leaf_attr, produced_refs, set())
            sources: list[dict[str, Any]] = []
            for leaf_index, col in sorted(leaves):
                input_name, table_id, is_dep = leaf_to_source.get(
                    leaf_index, (None, None, False)
                )
                sources.append({"input": input_name, "table": table_id, "column": col})
                if is_dep and table_id is not None:
                    consumed.setdefault(table_id, set()).add(col)
            columns[out_name] = {
                "sources": sources,
                "exact": bool(sources),
            }
        doc["columns"] = columns
        doc["consumed"] = {table: sorted(cols) for table, cols in consumed.items()}
    except Exception as exc:  # noqa: BLE001 — lineage is advisory, never fatal
        doc["warnings"].append(f"lineage capture failed: {exc}")
    return doc


__all__ = ["ENGINE", "capture_lineage"]

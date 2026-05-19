"""Postgres status page: schemas, tables, sample rows, and contract correlation."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app import cache
from utils.postgres import ColumnInfo, PostgresInspector, TableSnapshot

SCHEMA_ORDER: list[str] = ["bronze", "silver", "gold"]
SCHEMA_COLOR: dict[str, str] = {
    "bronze": "#b45309",
    "silver": "#94a3b8",
    "gold": "#eab308",
}


@st.cache_data(ttl=60)
def _cached_server_info() -> dict[str, str]:
    return PostgresInspector().server_info()


@st.cache_data(ttl=60, show_spinner="Inspecting warehouse…")
def _cached_snapshots() -> list[TableSnapshot]:
    return PostgresInspector().inspect(sample_size=10)


@st.cache_data(ttl=60)
def _cached_contract_index() -> dict[str, dict[str, Any]]:
    """Map table_name -> contract summary, or {} if MongoDB is unreachable."""
    try:
        return {c["table_name"]: c for c in cache.cached_summaries()}
    except Exception:
        return {}


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0:
            return f"{value:,.1f} {unit}"
        value /= 1024.0
    return f"{value:,.1f} PB"


def _cell(value: Any) -> Any:
    """Make a sample value safe for Arrow/dataframe rendering."""
    if isinstance(value, dict | list):
        return json.dumps(value, default=str)
    if isinstance(value, bytes | bytearray):
        return value.decode("utf-8", errors="replace")
    return value


def render() -> None:
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Postgres status</span>"
        "<span class='page-header-module'>warehouse explorer</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True):
            _cached_server_info.clear()
            _cached_snapshots.clear()
            _cached_contract_index.clear()
            cache.clear()
            st.rerun()

    try:
        info = _cached_server_info()
    except Exception as exc:
        st.error(f"Could not connect to PostgreSQL.\n\n`{type(exc).__name__}`: {exc}")
        return

    info_cols = st.columns(4)
    info_cols[0].metric("Host", info["host"])
    info_cols[1].metric("Database", info["database"])
    info_cols[2].metric("Port", info["port"])
    info_cols[3].metric("User", info["user"])

    try:
        snapshots = _cached_snapshots()
    except Exception as exc:
        st.error(f"Could not inspect tables: `{type(exc).__name__}`: {exc}")
        return

    if not snapshots:
        st.info("No user tables found in this database.")
        return

    contracts = _cached_contract_index()
    _render_summary(snapshots, contracts)

    by_schema: dict[str, list[TableSnapshot]] = {}
    for snap in snapshots:
        by_schema.setdefault(snap.schema, []).append(snap)

    ordered = [s for s in SCHEMA_ORDER if s in by_schema] + sorted(
        set(by_schema) - set(SCHEMA_ORDER)
    )
    for schema in ordered:
        _render_schema(schema, by_schema[schema], contracts)

    st.caption(f"PostgreSQL · {info['version']}")


def _render_summary(
    snapshots: list[TableSnapshot], contracts: dict[str, dict[str, Any]]
) -> None:
    total_rows = sum(s.row_count for s in snapshots)
    total_bytes = sum(s.size_bytes for s in snapshots)
    with_contract = sum(1 for s in snapshots if s.name in contracts)

    cols = st.columns(4)
    cols[0].metric("Tables", f"{len(snapshots):,}")
    cols[1].metric("Total rows", f"{total_rows:,}")
    cols[2].metric("Total size", _format_bytes(total_bytes))
    cols[3].metric("With contract", f"{with_contract}/{len(snapshots)}")

    if not contracts:
        st.warning(
            "Contracts could not be loaded from MongoDB — showing Postgres "
            "data only. Check `MONGO_URI` / `CONTRACTS_MONGO_URI`."
        )


def _render_schema(
    schema: str,
    tables: list[TableSnapshot],
    contracts: dict[str, dict[str, Any]],
) -> None:
    color = SCHEMA_COLOR.get(schema, "#6b7280")
    rows = sum(t.row_count for t in tables)
    st.markdown(
        f"<h3 style='border-color:{color}33;'>"
        f"<span style='color:{color};'>●</span> {schema}"
        f"<span style='color:#6b7280;font-weight:500;font-size:0.85rem;'>"
        f"  · {len(tables)} tables · {rows:,} rows</span></h3>",
        unsafe_allow_html=True,
    )
    for table in sorted(tables, key=lambda t: t.name):
        _render_table(table, contracts.get(table.name))


def _render_table(
    table: TableSnapshot, contract_summary: dict[str, Any] | None
) -> None:
    badge = "◆ contract" if contract_summary else "○ no contract"
    label = (
        f"{table.name}    {table.row_count:,} rows · "
        f"{_format_bytes(table.size_bytes)} · {len(table.columns)} cols    {badge}"
    )
    with st.expander(label):
        sample_tab, contract_tab = st.tabs(["Sample rows", "Contract"])
        with sample_tab:
            _render_sample(table)
        with contract_tab:
            _render_contract(table, contract_summary)


def _render_sample(table: TableSnapshot) -> None:
    if table.row_count == 0:
        st.info("Table is empty.")
        return
    if not table.sample_rows:
        st.info("No sample rows returned.")
        return
    df = pd.DataFrame(
        [{k: _cell(v) for k, v in row.items()} for row in table.sample_rows]
    )
    st.caption(f"{len(df)} random rows of {table.row_count:,}")
    st.dataframe(df, hide_index=True, use_container_width=True)


def _render_contract(
    table: TableSnapshot, contract_summary: dict[str, Any] | None
) -> None:
    if contract_summary is None:
        st.info(
            f"No contract found in MongoDB for `{table.name}`. "
            "Push contracts with the distributed pipeline test."
        )
        return

    try:
        contract = cache.cached_contract(table.name)
    except Exception as exc:
        st.error(f"Could not load contract: `{type(exc).__name__}`: {exc}")
        return

    comment = contract.get("comment")
    if comment:
        st.caption(comment)

    expectations = contract.get("expectations") or {}
    profile = contract.get("profile") or {}
    min_rows = expectations.get("min_rows")
    baseline = profile.get("row_count")

    cols = st.columns(3)
    cols[0].metric("Rows now", f"{table.row_count:,}")
    if min_rows is not None:
        meets = table.row_count >= min_rows
        cols[1].metric(
            "Min expected",
            f"{min_rows:,}",
            delta="meets" if meets else "below threshold",
            delta_color="normal" if meets else "inverse",
        )
    if baseline is not None:
        cols[2].metric(
            "Contract baseline",
            f"{baseline:,}",
            delta=f"{table.row_count - baseline:+,} vs baseline",
        )

    st.markdown("**Schema correlation**")
    st.dataframe(
        _correlate_schema(contract.get("fields") or [], table.columns),
        hide_index=True,
        use_container_width=True,
    )

    unique_keys = expectations.get("unique_keys") or []
    non_null = expectations.get("non_null_columns") or []
    if unique_keys:
        st.markdown("**Unique keys** · " + " · ".join("+".join(k) for k in unique_keys))
    if non_null:
        st.markdown("**Non-null columns** · " + ", ".join(non_null))

    st.caption(
        f"{contract.get('pipeline_key', '?')} · pushed {contract.get('pushed_at', '?')}"
    )


def _correlate_schema(
    contract_fields: list[dict[str, Any]], actual_columns: list[ColumnInfo]
) -> pd.DataFrame:
    """Build a row-by-row comparison of contract fields vs Postgres columns."""
    contract_by_name = {f["name"]: f for f in contract_fields}
    actual_by_name = {c.name: c for c in actual_columns}

    ordered_names = [f["name"] for f in contract_fields] + [
        c.name for c in actual_columns if c.name not in contract_by_name
    ]

    rows: list[dict[str, Any]] = []
    for name in ordered_names:
        field = contract_by_name.get(name)
        actual = actual_by_name.get(name)
        if field and actual:
            status = "✓ match"
        elif field and not actual:
            status = "✗ missing in table"
        else:
            status = "⚠ not in contract"
        rows.append(
            {
                "column": name,
                "contract type": field["type"] if field else "—",
                "postgres type": actual.data_type if actual else "—",
                "nullable": field.get("nullable") if field else None,
                "status": status,
            }
        )
    return pd.DataFrame(rows)

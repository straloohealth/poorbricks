"""Status dashboard: warehouse health, contract coverage, and sync freshness."""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from streamlit_app import cache
from utils.dates import hours_since
from utils.postgres import TableSnapshot

STALE_SYNC_HOURS: int = 48

# Histogram buckets for the last-sync distribution: (label, exclusive upper
# bound in hours). The final bucket is open-ended.
_SYNC_BUCKETS: list[tuple[str, float]] = [
    ("0–6h", 6.0),
    ("6–12h", 12.0),
    ("12–24h", 24.0),
    ("24–48h", 48.0),
    ("48h+", float("inf")),
]


def render() -> None:
    """Render the status dashboard page."""
    st.markdown(
        "<div class='page-header'>"
        "<span class='page-header-title'>Status</span>"
        "<span class='page-header-module'>warehouse + contract health</span>"
        "</div>",
        unsafe_allow_html=True,
    )

    col_refresh, _ = st.columns([1, 6])
    with col_refresh:
        if st.button("↻ Refresh", use_container_width=True):
            cache.clear()
            st.rerun()

    try:
        snapshots = cache.cached_warehouse_snapshots()
    except Exception as exc:
        st.error(f"Could not inspect the warehouse: `{type(exc).__name__}`: {exc}")
        return

    try:
        contracts = cache.cached_contract_details()
    except Exception as exc:
        st.warning(
            "Contracts could not be loaded from MongoDB — coverage and sync "
            f"freshness are unavailable. `{type(exc).__name__}`: {exc}"
        )
        contracts = []

    contract_index = {c.get("table_name") for c in contracts}

    _render_metrics(snapshots, contracts, contract_index)

    if snapshots:
        _render_contract_coverage(snapshots, contract_index)
        _render_empty_tables(snapshots)
    else:
        st.info(
            "No warehouse tables found. Populate them with "
            "`pytest tests/test_distributed_pipeline.py -m integration -n 0`."
        )

    _render_sync_freshness(contracts)


def _render_metrics(
    snapshots: list[TableSnapshot],
    contracts: list[dict[str, Any]],
    contract_index: set[str | None],
) -> None:
    """Render the four headline health metrics."""
    with_contract = sum(1 for s in snapshots if s.name in contract_index)
    empty = sum(1 for s in snapshots if s.row_count == 0)
    stale = sum(1 for _ in _stale_contracts(contracts))

    cols = st.columns(4)
    cols[0].metric("Warehouse tables", f"{len(snapshots):,}")
    cols[1].metric("With contract", f"{with_contract}/{len(snapshots)}")
    cols[2].metric("Empty tables", f"{empty:,}")
    cols[3].metric(f"Stale syncs (>{STALE_SYNC_HOURS}h)", f"{stale:,}")


def _render_contract_coverage(
    snapshots: list[TableSnapshot], contract_index: set[str | None]
) -> None:
    """Alert on warehouse tables that have no published contract."""
    st.markdown("### Databases without a contract")
    missing = sorted(
        (s for s in snapshots if s.name not in contract_index),
        key=lambda s: (s.schema, s.name),
    )
    if not missing:
        st.success("Every warehouse table has a published contract.")
        return
    st.error(
        f"{len(missing)} warehouse table(s) have no contract in MongoDB. "
        "Publish them with the distributed pipeline test."
    )
    st.dataframe(
        pd.DataFrame(
            [
                {"schema": s.schema, "table": s.name, "rows": s.row_count}
                for s in missing
            ]
        ),
        hide_index=True,
        use_container_width=True,
    )


def _render_empty_tables(snapshots: list[TableSnapshot]) -> None:
    """Alert on warehouse tables that hold zero rows."""
    st.markdown("### Empty databases")
    empty = sorted(
        (s for s in snapshots if s.row_count == 0),
        key=lambda s: (s.schema, s.name),
    )
    if not empty:
        st.success("No empty tables in the warehouse.")
        return
    st.error(f"{len(empty)} warehouse table(s) hold zero rows.")
    st.dataframe(
        pd.DataFrame([{"schema": s.schema, "table": s.name} for s in empty]),
        hide_index=True,
        use_container_width=True,
    )


def _render_sync_freshness(contracts: list[dict[str, Any]]) -> None:
    """Plot the last-sync age distribution and alert on stale contracts."""
    st.markdown("### Last sync distribution")
    if not contracts:
        st.info("No contracts available to chart.")
        return

    aged: list[tuple[str, float]] = []
    unknown = 0
    for contract in contracts:
        age = hours_since(contract.get("pushed_at"))
        if age is None:
            unknown += 1
            continue
        aged.append((contract.get("table_name", "?"), max(age, 0.0)))

    if not aged:
        st.info("No contracts have a parseable `pushed_at` timestamp.")
        return

    counts = {label: 0 for label, _ in _SYNC_BUCKETS}
    for _, hours in aged:
        for label, upper in _SYNC_BUCKETS:
            if hours < upper:
                counts[label] += 1
                break

    chart_df = pd.DataFrame(
        {"contracts": [counts[label] for label, _ in _SYNC_BUCKETS]},
        # Ordinal prefix keeps the buckets in chronological order on the axis.
        index=[f"{i + 1}. {label}" for i, (label, _) in enumerate(_SYNC_BUCKETS)],
    )
    st.bar_chart(chart_df, height=260)
    st.caption(
        f"Hours since each contract was last pushed · {len(aged)} contract(s) · "
        f"alert threshold {STALE_SYNC_HOURS}h"
        + (f" · {unknown} with no timestamp" if unknown else "")
    )

    stale = sorted(
        (pair for pair in aged if pair[1] > STALE_SYNC_HOURS),
        key=lambda pair: pair[1],
        reverse=True,
    )
    if not stale:
        st.success(f"All contracts synced within the last {STALE_SYNC_HOURS}h.")
        return
    st.error(f"{len(stale)} contract(s) have not synced in over {STALE_SYNC_HOURS}h.")
    st.dataframe(
        pd.DataFrame(
            [
                {"table": name, "hours since sync": round(hours, 1)}
                for name, hours in stale
            ]
        ),
        hide_index=True,
        use_container_width=True,
    )


def _stale_contracts(
    contracts: list[dict[str, Any]],
) -> list[tuple[str, float]]:
    """Return ``(table_name, hours)`` pairs for contracts older than the threshold."""
    stale: list[tuple[str, float]] = []
    for contract in contracts:
        age = hours_since(contract.get("pushed_at"))
        if age is not None and age > STALE_SYNC_HOURS:
            stale.append((contract.get("table_name", "?"), age))
    return stale
